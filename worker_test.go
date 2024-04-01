package bgjob_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/txix-open/bgjob"
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

	require.EqualValues(1, atomic.LoadInt32(&observer.jobCompeted))
	require.EqualValues(1, atomic.LoadInt32(&observer.jobWillBeRetried))
	require.EqualValues(1, atomic.LoadInt32(&observer.jobMovedToDlq))
	require.EqualValues(3, atomic.LoadInt32(&observer.jobStarted))
	require.GreaterOrEqual(atomic.LoadInt32(&observer.queueIsEmpty), int32(1))
	require.EqualValues(0, atomic.LoadInt32(&observer.workerError))
}

func TestWorker_Observer_Reschedule(t *testing.T) {
	require, _, cli := prepareTest(t)

	observer := &observerCounter{}

	rescheduled := int32(0)
	w := bgjob.NewWorker(cli, "test", bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		if atomic.LoadInt32(&rescheduled) == 3 {
			return bgjob.Complete()
		}
		atomic.AddInt32(&rescheduled, 1)
		return bgjob.Reschedule(1 * time.Second)
	}), bgjob.WithObserver(observer))
	w.Run(context.Background())
	time.Sleep(1 * time.Second) //trigger queue is empty

	err := cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Type:  "reschedule_me",
		Queue: "test",
	})
	require.NoError(err)
	time.Sleep(5 * time.Second)

	require.EqualValues(1, atomic.LoadInt32(&observer.jobCompeted))
	require.EqualValues(3, atomic.LoadInt32(&observer.jobRescheduled))
	require.EqualValues(4, atomic.LoadInt32(&observer.jobStarted))
	require.GreaterOrEqual(atomic.LoadInt32(&observer.queueIsEmpty), int32(1))
	require.EqualValues(0, atomic.LoadInt32(&observer.workerError))
}

func TestWorker_PollInterval(t *testing.T) {
	require, _, cli := prepareTest(t)
	observer := &observerCounter{}
	worker := bgjob.NewWorker(
		cli,
		"test",
		bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
			return bgjob.Complete()
		}),
		bgjob.WithPollInterval(2*time.Second),
		bgjob.WithObserver(observer),
	)
	worker.Run(context.Background())
	time.Sleep(5 * time.Second)
	require.EqualValues(3, atomic.LoadInt32(&observer.queueIsEmpty))
}

func TestWorker_RunHighConcurrency(t *testing.T) {
	require, _, cli := prepareTest(t)

	max := 1000
	c := make(chan bgjob.EnqueueRequest)
	jobCounter := sync.WaitGroup{}
	publishers := 16
	total := int32(0)
	for i := 0; i < publishers; i++ {
		go func() {
			for request := range c {
				err := cli.Enqueue(context.Background(), request)
				require.NoError(err)
			}
		}()
	}
	observer := &observerCounter{}
	handler := bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		jobCounter.Done()
		atomic.AddInt32(&total, 1)
		return bgjob.Complete()
	})
	worker := bgjob.NewWorker(
		cli,
		"test",
		handler,
		bgjob.WithConcurrency(32),
		bgjob.WithObserver(observer),
	)
	worker.Run(context.Background())

	start := time.Now()
	for i := 0; i < max; i++ {
		jobCounter.Add(1)
		c <- bgjob.EnqueueRequest{
			Queue: "test",
			Type:  "test",
		}
	}
	close(c)
	jobCounter.Wait()
	worker.Shutdown()
	require.EqualValues(max, atomic.LoadInt32(&total))
	dur := time.Since(start)
	t.Logf("%d jobs completed %s, rps:  %f", max, dur, float32(total)/float32(dur.Seconds()))

	require.EqualValues(0, atomic.LoadInt32(&observer.workerError))
}

type observerCounter struct {
	jobStarted       int32
	jobCompeted      int32
	jobWillBeRetried int32
	jobRescheduled   int32
	jobMovedToDlq    int32
	queueIsEmpty     int32
	workerError      int32
}

func (o *observerCounter) JobStarted(ctx context.Context, job bgjob.Job) {
	atomic.AddInt32(&o.jobStarted, 1)
}

func (o *observerCounter) JobCompleted(ctx context.Context, job bgjob.Job) {
	atomic.AddInt32(&o.jobCompeted, 1)
}

func (o *observerCounter) JobWillBeRetried(ctx context.Context, job bgjob.Job, after time.Duration, err error) {
	atomic.AddInt32(&o.jobWillBeRetried, 1)
}

func (o *observerCounter) JobRescheduled(ctx context.Context, job bgjob.Job, after time.Duration) {
	atomic.AddInt32(&o.jobRescheduled, 1)
}

func (o *observerCounter) JobMovedToDlq(ctx context.Context, job bgjob.Job, err error) {
	atomic.AddInt32(&o.jobMovedToDlq, 1)
}

func (o *observerCounter) QueueIsEmpty(ctx context.Context) {
	atomic.AddInt32(&o.queueIsEmpty, 1)
}

func (o *observerCounter) WorkerError(ctx context.Context, err error) {
	atomic.AddInt32(&o.workerError, 1)
}
