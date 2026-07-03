package source

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier is the DB-free testability seam (design §8). The production Source
// holds a pgxpool-backed Querier; tests inject a fakeQuerier that yields canned
// rows. This is THE single decision that lets unit tests run without Postgres.
//
// Exported so cmd/plugin can wire connectQuerier into DiscoverSchema's callback
// (DiscoverSchema itself is a standalone func taking this seam).
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (rowsIterator, error)
	Close()
}

// querier is a transparent alias for Querier, retained so existing internal
// references (the Source field, ConnectQuerier's callers, test fakes) read
// unchanged. DiscoverSchema's callback uses the exported Querier so cmd/plugin
// can wire ConnectQuerier from outside the package.
type querier = Querier

// rowsIterator mirrors the subset of pgx.Rows the Source needs. Named Values
// (not ScanValues) to match pgx.Rows.Values() exactly — heterogeneous []any
// per row, decoded via pgx's own type registry.
type rowsIterator interface {
	Next() bool
	Columns() []string
	Values() ([]any, error)
	Err() error
	Close()
}

// pgQuerier is the production querier, backed by a *pgxpool.Pool.
type pgQuerier struct {
	pool *pgxpool.Pool
}

func (q *pgQuerier) Query(ctx context.Context, sql string, args ...any) (rowsIterator, error) {
	rows, err := q.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgRows{rows: rows}, nil
}

func (q *pgQuerier) Close() { q.pool.Close() }

// pgRows adapts pgx.Rows to rowsIterator. Columns come from FieldDescriptions
// (the pgx-native API); Values() and Err() pass straight through.
type pgRows struct {
	rows pgx.Rows
}

func (r *pgRows) Next() bool { return r.rows.Next() }

func (r *pgRows) Columns() []string {
	fds := r.rows.FieldDescriptions()
	cols := make([]string, len(fds))
	for i, fd := range fds {
		cols[i] = fd.Name
	}
	return cols
}

func (r *pgRows) Values() ([]any, error) { return r.rows.Values() }
func (r *pgRows) Err() error             { return r.rows.Err() }
func (r *pgRows) Close()                 { r.rows.Close() }
