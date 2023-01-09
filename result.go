package bgjob

import (
	"time"
)

type Result struct {
	complete   bool
	err        error
	moveToDlq  bool
	retry      bool
	retryDelay time.Duration

	reschedule      bool
	rescheduleDelay time.Duration
}

func Complete() Result {
	return Result{complete: true}
}

func Retry(after time.Duration, err error) Result {
	return Result{retry: true, retryDelay: after, err: err}
}

func MoveToDlq(err error) Result {
	return Result{moveToDlq: true, err: err}
}

func Reschedule(after time.Duration) Result {
	return Result{reschedule: true, rescheduleDelay: after}
}
