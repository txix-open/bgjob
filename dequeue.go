package bgjob

import (
	"context"

	"github.com/pkg/errors"
)

func Dequeue(ctx context.Context, e ExecerContext, id string) error {
	return BulkDequeue(ctx, e, []string{id})
}

func BulkDequeue(ctx context.Context, e ExecerContext, ids []string) error {
	query := "DELETE FROM bgjob_job WHERE id=ANY($1);"
	_, err := e.ExecContext(ctx, query, ids)
	if err != nil {
		return errors.WithMessage(err, "exec delete bgjob")
	}
	return nil
}
