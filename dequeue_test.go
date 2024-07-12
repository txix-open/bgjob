package bgjob_test

import (
	"context"
	"testing"

	"github.com/txix-open/bgjob"
)

func TestDequeue(t *testing.T) {
	require, db, _ := prepareTest(t)

	err := bgjob.Enqueue(context.Background(), db, bgjob.EnqueueRequest{
		Id:    "123",
		Queue: "name",
		Type:  "test",
	})
	require.NoError(err)

	job, err := getJob(db.DB, "123")
	require.NoError(err)
	require.NotNil(job)

	err = bgjob.Dequeue(context.Background(), db, "123")
	require.NoError(err)

	_, err = getJob(db.DB, "123")
	require.Error(err)
}

func TestBulkDequeue(t *testing.T) {
	require, db, _ := prepareTest(t)

	err := bgjob.Enqueue(context.Background(), db, bgjob.EnqueueRequest{
		Id:    "123",
		Queue: "name",
		Type:  "test",
	})
	require.NoError(err)
	job, err := getJob(db.DB, "123")
	require.NoError(err)
	require.NotNil(job)

	err = bgjob.Enqueue(context.Background(), db, bgjob.EnqueueRequest{
		Id:    "125",
		Queue: "name",
		Type:  "test",
	})
	require.NoError(err)

	job, err = getJob(db.DB, "125")
	require.NoError(err)
	require.NotNil(job)

	err = bgjob.BulkDequeue(context.Background(), db, []string{"123", "125"})
	require.NoError(err)

	_, err = getJob(db.DB, "123")
	require.Error(err)
	_, err = getJob(db.DB, "125")
	require.Error(err)
}
