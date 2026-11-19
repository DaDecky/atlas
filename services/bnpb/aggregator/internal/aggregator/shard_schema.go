package aggregator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (s *Store) begin(ctx context.Context, readOnly bool) (pgx.Tx, error) {
	pool := s.ingest
	opts := pgx.TxOptions{}

	if readOnly {
		pool = s.query
		opts = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	}

	call := ctx

	cancel := func() {}
	if s.deadline > 0 {
		call, cancel = context.WithTimeout(ctx, s.deadline)
	}

	tx, err := pool.BeginTx(call, opts)

	cancel()

	if err != nil {
		return nil, err
	}

	if s.deadline > 0 {
		tx = &deadlineTx{Tx: tx, deadline: s.deadline}
	}

	if s.storeID != "" {
		var matches bool

		err = tx.QueryRow(ctx, `SELECT id=$1 FROM public.store_identity WHERE singleton`, s.storeID).Scan(&matches)
		if err != nil || !matches {
			_ = tx.Rollback(ctx)

			if err != nil {
				return nil, err
			}

			return nil, errors.New("store identity changed; copy needs refill")
		}
	}

	if s.namespace != "" {
		_, err = tx.Exec(ctx, `SELECT set_config('search_path', $1, true)`, pgx.Identifier{s.namespace}.Sanitize())
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, err
		}
	}

	return tx, nil
}

// initShard creates a private schema on a node using its owner's connection.
// Pools are shared across schemas; transactions set their own local search path.
func (s *Store) initShard(ctx context.Context, name string) (*Store, error) {
	tx, err := s.ingest.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(419031, 3)`); err != nil {
		return nil, err
	}

	ident := pgx.Identifier{name}.Sanitize()
	if _, err = tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+ident); err != nil {
		return nil, err
	}

	if _, err = tx.Exec(ctx, `REVOKE ALL ON SCHEMA `+ident+` FROM PUBLIC`); err != nil {
		return nil, err
	}

	if _, err = tx.Exec(ctx, `SELECT set_config('search_path',$1,true)`, ident); err != nil {
		return nil, err
	}

	if _, err = tx.Exec(ctx, schema); err != nil {
		return nil, err
	}
	// m2Schema is already an explicit transaction, so apply its statements inside ours.
	migration := strings.ReplaceAll(strings.ReplaceAll(m2Schema, "BEGIN;", ""), "COMMIT;", "")
	if _, err = tx.Exec(ctx, migration); err != nil {
		return nil, fmt.Errorf("shard migration: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &Store{ingest: s.ingest, query: s.query, namespace: name}, nil
}
