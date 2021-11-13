package bgjob

import (
	"context"
)

type Mux struct {
	data map[string]Handler
}

func NewMux() *Mux {
	return &Mux{
		data: make(map[string]Handler),
	}
}

func (m *Mux) Register(jobType string, handler Handler) *Mux {
	m.data[jobType] = handler
	return m
}

func (m *Mux) Handle(ctx context.Context, job Job) Result {
	handler, ok := m.data[job.Type]
	if !ok {
		return MoveToDlq(ErrUnknownType)
	}
	return handler.Handle(ctx, job)
}
