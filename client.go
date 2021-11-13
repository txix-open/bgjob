package bgjob

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

type Tx interface {
	Job() Job
	Update(ctx context.Context, id string, attempt int32, lastError string, nextRunAt int64) error
	Delete(ctx context.Context, id string) error
	SaveInDlq(ctx context.Context, job Job) error
}

type Store interface {
	Insert(ctx context.Context, job Job) error
	Acquire(ctx context.Context, queue string, tx func(tx Tx) error) error
}

type Client struct {
	store Store
}

func NewClient(store Store) *Client {
	return &Client{
		store: store,
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
	err := c.store.Insert(ctx, job)
	if err == ErrJobAlreadyExist {
		return err
	}
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	return nil
}

func (c *Client) Do(ctx context.Context, queue string, f func(ctx context.Context, job Job) Result) error {
	err := c.store.Acquire(ctx, queue, func(tx Tx) error {
		return c.jobTx(ctx, tx, f)
	})
	return err
}

func (c *Client) jobTx(ctx context.Context, tx Tx, f func(ctx context.Context, job Job) Result) error {
	job := tx.Job()
	job.Attempt++

	result := f(ctx, job)

	if result.complete {
		err := tx.Delete(ctx, job.Id)
		if err != nil {
			return fmt.Errorf("delete job: %w", err)
		}
	}

	if result.retry {
		err := tx.Update(
			ctx,
			job.Id,
			job.Attempt,
			result.err.Error(),
			timeNow().Add(result.retryDelay).Unix(),
		)
		if err != nil {
			return fmt.Errorf("update job: %w", err)
		}
	}

	if result.moveToDlq {
		errorString := result.err.Error()
		job.LastError = &errorString
		err := tx.SaveInDlq(ctx, job)
		if err != nil {
			return fmt.Errorf("insert into dlq: %w", err)
		}

		err = tx.Delete(ctx, job.Id)
		if err != nil {
			return fmt.Errorf("delete job: %w", err)
		}
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

func timeNow() time.Time {
	return time.Now().UTC()
}
