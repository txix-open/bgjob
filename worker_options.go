package bgjob

import (
	"time"
)

type WorkerOption func(w *Worker)

func WithPollInterval(interval time.Duration) WorkerOption {
	return func(w *Worker) {
		w.pollInterval = interval
	}
}

func WithConcurrency(value int) WorkerOption {
	return func(w *Worker) {
		w.concurrency = value
	}
}

func WithObserver(observer Observer) WorkerOption {
	return func(w *Worker) {
		w.observer = observer
	}
}
