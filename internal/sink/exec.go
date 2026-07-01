package sink

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// copyFromPool is the DB-free testability seam (design §8). It is the narrow
// interface around just the pgxpool.Pool method the Sink needs (CopyFrom) plus
// Close. The production Sink holds a copyFromPool (real = *pgxpool.Pool wrapped
// in pgPool); tests inject a fakePool that records the rows it received. This
// is THE single decision that lets unit tests run without Postgres.
type copyFromPool interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Close()
}

// pgPool is the production copyFromPool, backed by a *pgxpool.Pool.
// pgxpool.Pool.CopyFrom already matches the seam's signature exactly, so this
// is a thin wrapper that exists only so *pgxpool.Pool can satisfy copyFromPool
// (the interface is unexported; an unexported wrapper keeps pgxpool out of the
// Sink's field type and out of _test.go entirely).
type pgPool struct {
	pool *pgxpool.Pool
}

func (p *pgPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *pgPool) Close() { p.pool.Close() }
