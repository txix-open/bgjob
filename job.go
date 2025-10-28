package bgjob

import (
	"time"
)

type Job struct {
	Id        string
	Queue     string
	Type      string
	Arg       []byte
	Attempt   int32
	LastError *string
	NextRunAt int64
	CreatedAt time.Time
	UpdatedAt time.Time
	RequestId string
}
