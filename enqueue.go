package bgjob

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type ExecerContext interface {
	ExecContext(ctx context.Context, s string, args ...any) (sql.Result, error)
}

func Enqueue(ctx context.Context, e ExecerContext, req EnqueueRequest) error {
	return BulkEnqueue(ctx, e, []EnqueueRequest{req})
}

func BulkEnqueue(ctx context.Context, e ExecerContext, list []EnqueueRequest) error {
	jobs, err := requestsToJobs(list)
	if err != nil {
		return err
	}

	err = bulkInsert(ctx, e, jobs)
	if err == ErrJobAlreadyExist {
		return err
	}
	if err != nil {
		return fmt.Errorf("bulk insert: %w", err)
	}

	return nil
}

func requestsToJobs(list []EnqueueRequest) ([]Job, error) {
	if len(list) == 0 {
		return nil, errors.New("list is empty. at least one job is expected")
	}

	jobs := make([]Job, 0, len(list))
	now := timeNow()
	for _, req := range list {
		if req.Queue == "" {
			return nil, ErrQueueIsRequired
		}
		if req.Type == "" {
			return nil, ErrTypeIsRequired
		}

		id := req.Id
		if id == "" {
			generated, err := nextId()
			if err != nil {
				return nil, fmt.Errorf("generate id: %w", err)
			}
			id = generated
		}
		job := Job{
			Id:        id,
			Queue:     req.Queue,
			Type:      req.Type,
			Arg:       req.Arg,
			RequestId: req.RequestId,
			Attempt:   0,
			LastError: nil,
			NextRunAt: now.Add(req.Delay).Unix(),
			CreatedAt: now,
			UpdatedAt: now,
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func bulkInsert(ctx context.Context, e ExecerContext, jobs []Job) error {
	valueStrings := make([]string, 0, len(jobs))
	valueArgs := make([]any, 0, len(jobs)*9)
	placeholderNum := 0
	for _, job := range jobs {
		placeholders := make([]string, 0)
		for i := 0; i < 9; i++ {
			placeholderNum++
			placeholders = append(placeholders, fmt.Sprintf("$%d", placeholderNum))
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))
		valueArgs = append(
			valueArgs,
			job.Id,
			job.Queue,
			job.Type,
			job.Arg,
			job.Attempt,
			job.NextRunAt,
			job.CreatedAt,
			job.UpdatedAt,
			job.RequestId,
		)
	}
	query := fmt.Sprintf("INSERT INTO bgjob_job (id, queue, type, arg, attempt, next_run_at, created_at, updated_at, request_id) VALUES %s",
		strings.Join(valueStrings, ","))
	_, err := e.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		if strings.Contains(err.Error(), "23505") { //unique_violation
			return ErrJobAlreadyExist
		}
		return err
	}
	return nil
}
