package bgjob

import (
	"context"
	"sync"
	"time"
)

type Handler interface {
	Handle(ctx context.Context, job Job) Result
}

type HandlerFunc func(ctx context.Context, job Job) Result

func (h HandlerFunc) Handle(ctx context.Context, job Job) Result {
	return h(ctx, job)
}

type Worker struct {
	cli          *Client
	queue        string
	handler      Handler
	pollInterval time.Duration
	concurrency  int
	wg           *sync.WaitGroup
	observer     Observer
	close        chan struct{}
}

func NewWorker(cli *Client, queue string, handler Handler, opts ...WorkerOption) *Worker {
	w := &Worker{
		cli:          cli,
		queue:        queue,
		handler:      handler,
		pollInterval: 1 * time.Second,
		concurrency:  1,
		wg:           &sync.WaitGroup{},
		close:        make(chan struct{}),
		observer:     NewNoopObserver(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (w *Worker) Run(ctx context.Context) {
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.run(ctx)
	}
}

func (w *Worker) run(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case <-w.close:
			return
		default:
		}

		var lastResult Result
		var lastJob Job
		err := w.cli.Do(ctx, w.queue, func(ctx context.Context, job Job) Result {
			w.observer.JobStarted(ctx, job)

			result := w.handler.Handle(ctx, job)
			lastResult = result
			lastJob = job

			return result
		})
		if err != nil {
			if err == ErrEmptyQueue {
				w.observer.QueueIsEmpty(ctx)
			} else {
				w.observer.WorkerError(ctx, err)
			}

			select {
			case <-ctx.Done():
				return
			case <-w.close:
				return
			case <-time.After(w.pollInterval):

			}
			continue
		}

		if lastResult.complete {
			w.observer.JobCompleted(ctx, lastJob)
		}
		if lastResult.retry {
			w.observer.JobWillBeRetried(ctx, lastJob, lastResult.retryDelay, lastResult.err)
		}
		if lastResult.moveToDlq {
			w.observer.JobMovedToDlq(ctx, lastJob, lastResult.err)
		}
		if lastResult.reschedule {
			w.observer.JobRescheduled(ctx, lastJob, lastResult.rescheduleDelay)
		}
	}
}

func (w *Worker) Shutdown() {
	close(w.close)
	w.wg.Wait()
}
