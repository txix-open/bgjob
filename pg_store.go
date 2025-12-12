package bgjob

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type pgStore struct {
	db *sql.DB
}

func NewPgStoreV2(ctx context.Context, db *sql.DB) (*pgStore, error) {
	err := assertHasColumn(ctx, db, "bgjob_job", "request_id")
	if err != nil {
		return nil, fmt.Errorf("%w; please apply 'add_request_id.sql' migration adding column `request_id` to `bgjob_job`", err)
	}

	err = assertHasColumn(ctx, db, "bgjob_dead_job", "request_id")
	if err != nil {
		return nil, fmt.Errorf("%w; please apply 'add_request_id.sql' migration adding column `request_id` to `bgjob_dead_job`", err)
	}

	return &pgStore{
			db: db,
		},
		nil
}

func (p *pgStore) BulkInsert(ctx context.Context, jobs []Job) error {
	return bulkInsert(ctx, p.db, jobs)
}

func (p *pgStore) Acquire(ctx context.Context, queue string, handler func(tx Tx) error) error {
	return runTx(ctx, p.db, func(ctx context.Context, tx *sql.Tx) error {
		query := `
SELECT id, queue, type, arg, attempt, last_error, next_run_at, created_at, updated_at, request_id
FROM bgjob_job
WHERE queue = $1 AND next_run_at <= $2
ORDER BY next_run_at, created_at
LIMIT 1 FOR UPDATE SKIP LOCKED
`
		now := timeNow().Unix()
		job := Job{}
		err := tx.QueryRowContext(ctx, query, queue, now).Scan(
			&job.Id,
			&job.Queue,
			&job.Type,
			&job.Arg,
			&job.Attempt,
			&job.LastError,
			&job.NextRunAt,
			&job.CreatedAt,
			&job.UpdatedAt,
			&job.RequestId,
		)
		if err == sql.ErrNoRows {
			return ErrEmptyQueue
		}
		if err != nil {
			return err
		}
		return handler(&pgTx{
			job: job,
			tx:  tx,
		})
	})
}

func runTx(ctx context.Context, db *sql.DB, txFunc func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				err = fmt.Errorf("%w, rollback error: %v", err, rbErr.Error())
			}
		} else {
			comErr := tx.Commit()
			if comErr != nil {
				err = fmt.Errorf("commit: %w", comErr)
			}
		}
	}()

	return txFunc(ctx, tx)
}

type pgTx struct {
	job Job
	tx  *sql.Tx
}

func (p *pgTx) Job() Job {
	return p.job
}

func (p *pgTx) Update(ctx context.Context, id string, attempt int32, lastError string, nextRunAt int64) error {
	query := "UPDATE bgjob_job SET attempt = $1, last_error = $2, next_run_at = $3, updated_at = $4 WHERE id = $5"
	if len(lastError) > 0 {
		lastError = "<" + timeNow().Format(time.RFC3339) + "> " + lastError
	}
	_, err := p.tx.ExecContext(ctx, query, attempt, lastError, nextRunAt, timeNow(), id)
	return err
}

func (p *pgTx) UpdateNextRun(ctx context.Context, id string, nextRunAt int64) error {
	query := "UPDATE bgjob_job SET next_run_at = $1, updated_at = $2 WHERE id = $3"
	_, err := p.tx.ExecContext(ctx, query, nextRunAt, timeNow(), id)
	return err
}

func (p *pgTx) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM bgjob_job WHERE id = $1`
	_, err := p.tx.ExecContext(ctx, query, id)
	return err
}

func (p *pgTx) SaveInDlq(ctx context.Context, job Job) error {
	query := `INSERT INTO bgjob_dead_job 
(job_id, queue, type, arg, attempt, next_run_at, last_error, job_created_at, job_updated_at, moved_at, request_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`
	_, err := p.tx.ExecContext(
		ctx,
		query,
		job.Id,
		job.Queue,
		job.Type,
		job.Arg,
		job.Attempt,
		job.NextRunAt,
		job.LastError,
		job.CreatedAt,
		job.UpdatedAt,
		timeNow(),
		job.RequestId,
	)
	return err
}

func (p *pgTx) UpdateArg(ctx context.Context, id string, arg []byte) error {
	query := "UPDATE bgjob_job SET arg = $1, updated_at = $2 WHERE id = $3"
	_, err := p.tx.ExecContext(ctx, query, arg, timeNow(), id)
	return err
}

func assertHasColumn(ctx context.Context, db *sql.DB, table, column string) error {
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT 1", column, table)

	err := db.QueryRowContext(ctx, query).Scan(new(any))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("bgjob: schema check error for %s.%s: %w", table, column, err)
	}

	return nil
}
