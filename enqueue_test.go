package bgjob_test

import (
	"context"
	"testing"

	"github.com/txix-open/bgjob"
)

func TestEnqueueTxRollback(t *testing.T) {
	require, db, _ := prepareTest(t)

	tx, err := db.Begin()
	require.NoError(err)

	req := bgjob.EnqueueRequest{
		Id:    "123",
		Queue: "name",
		Type:  "test",
	}
	err = bgjob.Enqueue(context.Background(), tx, req)
	require.NoError(err)

	err = tx.Rollback()
	require.NoError(err)

	_, err = getJob(db.DB, "123")
	require.Error(err)
}

func TestEnqueueTxCommit(t *testing.T) {
	require, db, _ := prepareTest(t)

	tx, err := db.Begin()
	require.NoError(err)

	req1 := bgjob.EnqueueRequest{
		Id:    "123",
		Queue: "name",
		Type:  "test",
	}
	req2 := bgjob.EnqueueRequest{
		Id:    "124",
		Queue: "name",
		Type:  "test",
	}
	err = bgjob.Enqueue(context.Background(), tx, req1)
	require.NoError(err)
	err = bgjob.Enqueue(context.Background(), tx, req2)
	require.NoError(err)

	_, err = getJob(db.DB, "123")
	require.Error(err)

	err = tx.Commit()
	require.NoError(err)

	job1, err := getJob(db.DB, "123")
	require.NoError(err)
	require.NotNil(job1)
	job2, err := getJob(db.DB, "124")
	require.NoError(err)
	require.NotNil(job2)
}
