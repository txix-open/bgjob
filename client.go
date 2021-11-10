package bgjob

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

var (
	ErrQueueIsRequired = errors.New("queue is required")
	ErrTypeIsRequired  = errors.New("type is required")
	ErrEmptyQueue      = errors.New("queue is empty")
)

type Client struct {
	db *sql.DB
}

func NewClient(db *sql.DB) *Client {
	return &Client{
		db: db,
	}
}

func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) error {
	if req.Queue == "" {
		return ErrQueueIsRequired
	}
	if req.Type == "" {
		return ErrTypeIsRequired
	}

	now := timeNow()
	id := req.Id
	if id == "" {
		generated, err := nextId()
		if err != nil {
			return fmt.Errorf("generate id: %w", err)
		}
		id = generated
	}
	job := Job{
		Id:        id,
		Queue:     req.Queue,
		Type:      req.Type,
		Arg:       req.Arg,
		Attempt:   0,
		LastError: nil,
		NextRunAt: now.Add(req.Delay).Unix(),
		CreatedAt: now,
		UpdatedAt: now,
	}
	query := `INSERT INTO bgjob_job 
(id, queue, type, arg, attempt, next_run_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`
	_, err := c.db.ExecContext(
		ctx,
		query,
		job.Id,
		job.Queue,
		job.Type,
		job.Arg,
		job.Attempt,
		job.NextRunAt,
		job.CreatedAt,
		job.UpdatedAt,
	) //TODO handle conflict and return specified error
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	return nil
}

func (c *Client) Do(ctx context.Context, queue string, f func(ctx context.Context, job Job) Result) error {
	err := runTx(ctx, c.db, func(ctx context.Context, tx *sql.Tx) error {
		query := `
SELECT id, queue, type, arg, attempt, last_error, next_run_at, created_at, updated_at
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
		)
		if err == sql.ErrNoRows {
			return ErrEmptyQueue
		}
		if err != nil {
			return fmt.Errorf("select job: %w", err)
		}

		job.Attempt++
		result := f(ctx, job)

		if result.complete {
			query := `DELETE FROM bgjob_job WHERE id = $1`
			_, err := tx.ExecContext(ctx, query, job.Id)
			if err != nil {
				return fmt.Errorf("delete job %s: %w", job.Id, err)
			}
			return nil
		}

		if result.retry {
			query := `
	UPDATE bgjob_job SET attempt = $1, last_error = $2, next_run_at = $3, updated_at = $4 WHERE id = $5
`
			now := timeNow()
			_, err := tx.ExecContext(
				ctx,
				query,
				job.Attempt,
				result.err.Error(),
				now.Add(result.retryDelay).Unix(),
				now,
				job.Id,
			)
			if err != nil {
				return fmt.Errorf("update job %s: %w", job.Id, err)
			}
			return nil
		}

		if result.moveToDlq {
			query := `INSERT INTO bgjob_dead_job 
(job_id, queue, type, arg, attempt, next_run_at, last_error, job_created_at, job_updated_at, moved_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
`
			_, err := c.db.ExecContext(
				ctx,
				query,
				job.Id,
				job.Queue,
				job.Type,
				job.Arg,
				job.Attempt,
				job.NextRunAt,
				result.err.Error(),
				job.CreatedAt,
				job.UpdatedAt,
				timeNow(),
			)
			if err != nil {
				return fmt.Errorf("insert dead job: %w", err)
			}

			query = `DELETE FROM bgjob_job WHERE id = $1`
			_, err = tx.ExecContext(ctx, query, job.Id)
			if err != nil {
				return fmt.Errorf("delete job %s: %w", job.Id, err)
			}
			return nil

		}

		return nil
	})
	if err == ErrEmptyQueue {
		return err
	}
	if err != nil {
		return fmt.Errorf("do job: %w", err)
	}
	return nil
}

func nextId() (string, error) {
	arr := make([]byte, 24)
	_, err := rand.Read(arr)
	if err != nil {
		return "", fmt.Errorf("read rand: %w", err)
	}
	id := hex.EncodeToString(arr)
	return id, nil
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
			err := tx.Commit()
			if err != nil {
				err = fmt.Errorf("commit: %w", err)
			}
		}
	}()

	return txFunc(ctx, tx)
}

func timeNow() time.Time {
	return time.Now().UTC()
}
