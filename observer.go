package bgjob

import (
	"context"
	"time"
)

type Observer interface {
	JobStarted(ctx context.Context, job Job)
	JobCompleted(ctx context.Context, job Job)
	JobRescheduled(ctx context.Context, job Job, after time.Duration)
	JobWillBeRetried(ctx context.Context, job Job, after time.Duration, err error)
	JobMovedToDlq(ctx context.Context, job Job, err error)
	QueueIsEmpty(ctx context.Context)
	WorkerError(ctx context.Context, err error)
}

type NoopObserver struct {
}

func (n NoopObserver) JobStarted(ctx context.Context, job Job) {
}

func (n NoopObserver) JobCompleted(ctx context.Context, job Job) {
}

func (n NoopObserver) JobWillBeRetried(ctx context.Context, job Job, after time.Duration, err error) {
}

func (n NoopObserver) JobRescheduled(ctx context.Context, job Job, after time.Duration) {
}

func (n NoopObserver) JobMovedToDlq(ctx context.Context, job Job, err error) {
}

func (n NoopObserver) QueueIsEmpty(ctx context.Context) {

}

func (n NoopObserver) WorkerError(ctx context.Context, err error) {
}

func NewNoopObserver() NoopObserver {
	return NoopObserver{}
}
