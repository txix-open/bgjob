package bgjob

import (
	"time"
)

type EnqueueRequest struct {
	Id        string        //optional
	Queue     string        //required
	Type      string        //required
	Arg       []byte        //optional
	Delay     time.Duration //optional
	RequestId string        //required
}
