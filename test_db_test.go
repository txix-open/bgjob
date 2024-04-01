package bgjob_test

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
)

type db struct {
	t         *testing.T
	defaultDb *sql.DB
	schema    string
	*sql.DB
}

func Open(dsn string, t *testing.T) (*db, error) {
	schema := strings.ToLower(t.Name())
	defaultDb, err := sql.Open("pgx", dsn)
	defaultDb.SetMaxOpenConns(1)
	if err != nil {
		return nil, errors.WithMessage(err, "open")
	}
	err = defaultDb.Ping()
	if err != nil {
		return nil, errors.WithMessage(err, "ping")
	}

	_, err = defaultDb.Exec(fmt.Sprintf("CREATE SCHEMA %s", schema))
	if err != nil {
		return nil, errors.WithMessage(err, "create schema")
	}

	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, errors.WithMessage(err, "parse dsn")
	}
	query, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, errors.WithMessage(err, "parse query")
	}
	query.Set("search_path", schema)
	uri.RawQuery = query.Encode()

	tempDb, err := sql.Open("pgx", uri.String())
	if err != nil {
		return nil, errors.WithMessage(err, "open")
	}
	err = tempDb.Ping()
	if err != nil {
		return nil, errors.WithMessage(err, "ping")
	}

	db := &db{
		t:         t,
		defaultDb: defaultDb,
		DB:        tempDb,
		schema:    schema,
	}

	return db, nil
}

func (db *db) Close() error {
	_, err := db.defaultDb.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", db.schema))
	if err != nil {
		return errors.WithMessage(err, "drop schema")
	}

	_ = db.defaultDb.Close()
	_ = db.DB.Close()

	return nil
}
