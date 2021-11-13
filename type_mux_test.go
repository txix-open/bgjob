package bgjob_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integration-system/bgjob"
)

func TestMux(t *testing.T) {
	require, db, cli := prepareTest(t)

	value := int32(0)
	handler := bgjob.HandlerFunc(func(ctx context.Context, job bgjob.Job) bgjob.Result {
		atomic.AddInt32(&value, 1)
		return bgjob.Complete()
	})

	mux := bgjob.NewMux().
		Register("type1", handler).
		Register("type2", handler)
	w := bgjob.NewWorker(cli, "test", mux)
	w.Run(context.Background())

	err := cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Id:    "1",
		Type:  "type1",
		Queue: "test",
	})
	require.NoError(err)
	err = cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Id:    "2",
		Type:  "type2",
		Queue: "test",
	})
	require.NoError(err)
	err = cli.Enqueue(context.Background(), bgjob.EnqueueRequest{
		Id:    "3",
		Type:  "type3",
		Queue: "test",
	})
	require.NoError(err)

	time.Sleep(3 * time.Second)

	require.EqualValues(2, atomic.LoadInt32(&value))

	job, err := getDeadJob(db.DB, "3")
	require.NoError(err)
	require.EqualValues("type3", job.Type)
	require.EqualValues(bgjob.ErrUnknownType.Error(), *job.LastError)

}
