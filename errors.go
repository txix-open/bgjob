package bgjob

import (
	"errors"
)

var (
	ErrQueueIsRequired     = errors.New("queue is required")
	ErrTypeIsRequired      = errors.New("type is required")
	ErrEmptyQueue          = errors.New("queue is empty")
	ErrJobAlreadyExist     = errors.New("job already exist")
	ErrRequestIdIsRequired = errors.New("request_id is required")

	ErrUnknownType = errors.New("unknown type")
)
