package aggregator

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// A refill may take many batches. Bound each database operation rather than
// cutting off the entire healthy refill after a single shard deadline.
type deadlineTx struct {
	pgx.Tx
	deadline time.Duration
}

type deadlineRows struct {
	pgx.Rows
	cancel context.CancelFunc
}

type deadlineRow struct {
	pgx.Row
	cancel context.CancelFunc
}

func (t *deadlineTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	call, cancel := context.WithTimeout(ctx, t.deadline)
	defer cancel()

	return t.Tx.Exec(call, sql, args...)
}

func (t *deadlineTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	call, cancel := context.WithTimeout(ctx, t.deadline)

	rows, err := t.Tx.Query(call, sql, args...)
	if err != nil {
		cancel()
		return nil, err
	}

	return &deadlineRows{rows, cancel}, nil
}

func (t *deadlineTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	call, cancel := context.WithTimeout(ctx, t.deadline)
	return &deadlineRow{t.Tx.QueryRow(call, sql, args...), cancel}
}

func (t *deadlineTx) Commit(ctx context.Context) error {
	call, cancel := context.WithTimeout(ctx, t.deadline)
	defer cancel()

	return t.Tx.Commit(call)
}

func (t *deadlineTx) Rollback(ctx context.Context) error {
	call, cancel := context.WithTimeout(ctx, t.deadline)
	defer cancel()

	return t.Tx.Rollback(call)
}

func (r *deadlineRows) Close() {
	r.Rows.Close()
	r.cancel()
}

func (r *deadlineRows) Next() bool {
	if r.Rows.Next() {
		return true
	}

	r.cancel()

	return false
}

func (r *deadlineRow) Scan(dest ...any) error {
	defer r.cancel()
	return r.Row.Scan(dest...)
}
