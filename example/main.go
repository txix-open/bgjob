package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"runtime"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/integration-system/bgjob"
)

type Observer struct {
	bgjob.NoopObserver //"extend" NoopObserver to override method you need
}

func (o Observer) WorkerError(ctx context.Context, err error) {
	log.Printf("worker: %v", err)
}

func main() {
	dsn := "postgres://test:test@localhost:5432/test"
	db, err := sql.Open("pgx", dsn) //be sure you add tables and indexes from migration/init.sql
	if err != nil {
		panic(err)
	}

	handleComplete := bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		fmt.Printf("%s\n", job.Arg)
		return bgjob.Complete() //complete job, job will be deleted
	})
	handleRetry := bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		if job.Attempt == 2 {
			return bgjob.Complete()
		}
		//here you have attempts counter, you can easily do backoff retries
		return bgjob.Retry(1*time.Second, errors.New("some error"))
	})
	handleMoveToDlq := bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		//you can move the job to permanent storage if you can't handle it
		return bgjob.MoveToDlq(errors.New("move to dlq"))
	})

	cli := bgjob.NewClient(bgjob.NewPgStore(db))

	//if handler for job type wasn't provided, job will be moved to dlq
	handler := bgjob.NewMux().
		Register("complete_me", handleComplete).
		Register("retry_me", handleRetry).
		Register("move_to_dlq", handleMoveToDlq)
	queueName := "test"
	observer := Observer{}
	worker := bgjob.NewWorker(
		cli,
		queueName,
		handler,
		bgjob.WithObserver(observer),            //default noop
		bgjob.WithConcurrency(runtime.NumCPU()), //default 1
		bgjob.WithPollInterval(500*time.Millisecond), //default 1s
	)
	ctx := context.Background()
	worker.Run(ctx) //call ones, non-blocking

	err = cli.Enqueue(ctx, bgjob.EnqueueRequest{
		Id:    "test", //you can provide your own id
		Queue: queueName,
		Type:  "complete_me",
		Arg:   []byte(`{"simpleJson": 1}`), //it can be json or protobuf or a simple string
		Delay: 1 * time.Second,             //you can delay job execution
	})
	if err != nil {
		panic(err)
	}

	err = cli.Enqueue(ctx, bgjob.EnqueueRequest{
		Queue: queueName,
		Type:  "retry_me",
		//those fields must be specified
	})
	if err != nil {
		panic(err)
	}

	err = cli.Enqueue(ctx, bgjob.EnqueueRequest{
		Queue: queueName,
		Type:  "move_to_dlq",
	})
	if err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)
	worker.Shutdown() //graceful shutdown, call ones
}

func enqueueInTx(db *sql.DB) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// YOUR BUSINESS TRANSACTION HERE

	err = bgjob.Enqueue(context.Background(), tx, bgjob.EnqueueRequest{
		Queue: "work",
		Type:  "send_email",
	})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}
