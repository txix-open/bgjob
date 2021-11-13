package bgjob_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integration-system/bgjob"
	"github.com/pkg/errors"
)

func TestWorker_Run(t *testing.T) {
	require, _, cli := prepareTest(t)

	value := int32(0)
	w := bgjob.NewWorker(cli, "test", bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		atomic.AddInt32(&value, 1)
		return bgjob.Complete()
	}))
	w.Run(context.Background())

	err := cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Type:  "test",
		Queue: "test",
	})
	require.NoError(err)
	time.Sleep(3 * time.Second)
	require.EqualValues(1, atomic.LoadInt32(&value))
}

func TestWorker_Shutdown(t *testing.T) {
	require, _, cli := prepareTest(t)

	value := int32(0)
	w := bgjob.NewWorker(cli, "test", bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		time.Sleep(3 * time.Second)
		atomic.AddInt32(&value, 1)
		return bgjob.Complete()
	}))
	w.Run(context.Background())

	err := cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Type:  "test",
		Queue: "test",
	})
	require.NoError(err)
	time.Sleep(1 * time.Second)
	w.Shutdown()
	require.EqualValues(1, atomic.LoadInt32(&value))
}

func TestWorker_RunConcurrency(t *testing.T) {
	require, _, cli := prepareTest(t)

	value := int32(0)
	w := bgjob.NewWorker(cli, "test", bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		time.Sleep(5 * time.Second)
		atomic.AddInt32(&value, 1)
		return bgjob.Complete()
	}), bgjob.WithConcurrency(2))
	w.Run(context.Background())

	err := cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Type:  "test",
		Queue: "test",
	})
	require.NoError(err)
	err = cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Type:  "test",
		Queue: "test",
	})
	require.NoError(err)

	time.Sleep(6 * time.Second) //6 < 10
	require.EqualValues(2, atomic.LoadInt32(&value))

}

func TestWorker_Observer(t *testing.T) {
	require, _, cli := prepareTest(t)

	observer := &observerCounter{}
	w := bgjob.NewWorker(cli, "test", bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		if job.Type == "complete_me" {
			return bgjob.Complete()
		}
		if job.Attempt == 1 {
			return bgjob.Retry(0, errors.New("retry"))
		}
		if job.Attempt == 2 {
			return bgjob.MoveToDlq(errors.New("to dlq"))
		}
		return bgjob.Complete()
	}), bgjob.WithObserver(observer))
	w.Run(context.Background())
	time.Sleep(1 * time.Second) //trigger queue is empty

	err := cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Type:  "not_compete",
		Queue: "test",
	})
	require.NoError(err)
	err = cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Type:  "complete_me",
		Queue: "test",
	})
	require.NoError(err)
	time.Sleep(3 * time.Second)

	require.EqualValues(1, observer.jobCompeted)
	require.EqualValues(1, observer.jobWillBeRetried)
	require.EqualValues(1, observer.jobMovedToDlq)
	require.EqualValues(3, observer.jobStarted)
	require.GreaterOrEqual(observer.queueIsEmpty, 1)
	require.EqualValues(0, observer.workerError)
}

type observerCounter struct {
	jobStarted       int
	jobCompeted      int
	jobWillBeRetried int
	jobMovedToDlq    int
	queueIsEmpty     int
	workerError      int
}

func (o *observerCounter) JobStarted(ctx context.Context, job bgjob.Job) {
	o.jobStarted++
}

func (o *observerCounter) JobCompleted(ctx context.Context, job bgjob.Job) {
	o.jobCompeted++
}

func (o *observerCounter) JobWillBeRetried(ctx context.Context, job bgjob.Job, after time.Duration, err error) {
	o.jobWillBeRetried++
}

func (o *observerCounter) JobMovedToDlq(ctx context.Context, job bgjob.Job, err error) {
	o.jobMovedToDlq++
}

func (o *observerCounter) QueueIsEmpty(ctx context.Context) {
	o.queueIsEmpty++
}

func (o *observerCounter) WorkerError(ctx context.Context, err error) {
	o.workerError++
}
