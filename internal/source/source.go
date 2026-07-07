// Package source implements the postgres-source component: a batch SELECT
// reader that emits dbbatch.DBBatch payloads (design §4). It reads rows through
// a querier seam so unit tests need no real Postgres.
package source

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/planx-lab/planx-plugin-postgres/internal/dbbatch"
	"github.com/planx-lab/planx-plugin-postgres/internal/dbcommon"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// Config configures the postgres-source component (design §4 ConfigSchema).
// The raw-SQL `query` field was replaced (ADR-013) by `table`+`columns`: the
// Source now builds `SELECT {cols} FROM {table}` internally so no user-supplied
// SQL reaches the DB. DiscoverSchema introspects information_schema to guide
// table/column selection in the Designer.
//
// The shared connection fields live in dbcommon.DSNConfig (embedded); source
// adds Columns (required) and BatchRows.
type Config struct {
	dbcommon.DSNConfig
	Columns   string `json:"columns"`
	BatchRows int    `json:"batchRows"`
}

const defaultBatchRows = 1000

// Source reads a finite SELECT result set in row batches. EOF terminates the
// stream so the DAG runtime reaches SUCCEEDED.
type Source struct {
	cfg     Config
	q       querier
	rows    rowsIterator
	columns []string
	done    bool
}

// New returns a zero-value Source satisfying sdk.SourceSPI.
func New() sdk.SourceSPI { return &Source{} }

// dbbatch gob registration is centralized in the dbbatch package's init()
// (gob.RegisterName under a shared wire name for cross-connector interop).

// Init parses config, validates required fields, applies defaults, connects
// (pgxpool), and issues the SELECT — built internally from table+columns
// (ADR-013) — to obtain a rows cursor. No user-supplied SQL reaches the DB.
func (s *Source) Init(ctx context.Context, cfg []byte) error {
	if err := dbcommon.Parse(string(cfg), "postgres source", &s.cfg); err != nil {
		return err
	}
	if err := dbcommon.ValidateCommon(s.cfg.DSNConfig, "postgres source"); err != nil {
		return err
	}
	if s.cfg.Columns == "" {
		// Empty columns used to fall back to "SELECT *" — dangerous if the
		// upstream schema changes (silent pipeline breakage). Require an
		// explicit column selection.
		return fmt.Errorf("postgres source: columns is required — select at least one column")
	}
	dbcommon.ApplyDefaults(&s.cfg.DSNConfig)
	if s.cfg.BatchRows <= 0 {
		s.cfg.BatchRows = defaultBatchRows
	}

	q, err := ConnectQuerier(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.q = q

	cols := s.cfg.Columns
	query := fmt.Sprintf("SELECT %s FROM %s", cols, s.cfg.Table)
	rows, err := s.q.Query(context.Background(), query)
	if err != nil {
		s.q.Close()
		return fmt.Errorf("postgres source: query: %w", err)
	}
	s.rows = rows
	s.columns = rows.Columns()
	return nil
}

// ConnectQuerier builds the DSN, opens a pgxpool, and pings. Extracted from
// Init so DiscoverSchema can reuse the same connect path against a temporary
// pool (opened, queried, closed) without constructing a Source.
func ConnectQuerier(ctx context.Context, cfg Config) (Querier, error) {
	dsn := dbcommon.BuildDSN(cfg.DSNConfig)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres source: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres source: ping: %w", err)
	}
	return &pgQuerier{pool: pool}, nil
}

// ReadBatch reads up to BatchRows records and returns them as a dbbatch.DBBatch.
// Two-phase EOF (design §4) — MANDATORY:
//   - a partial trailing batch is returned first with nil error, THEN io.EOF on
//     the next call. Returning io.EOF while rows are buffered drops data.
//   - an empty result yields io.EOF immediately.
//   - after the rows.Next() loop, rows.Err() is checked to surface delayed
//     driver errors (pgx surfaces them there, not from Next).
func (s *Source) ReadBatch() (sdk.Batch, error) {
	batch := make([]dbbatch.DBRow, 0, s.cfg.BatchRows)
	for len(batch) < s.cfg.BatchRows {
		if !s.rows.Next() {
			s.done = true
			break
		}
		vals, err := s.rows.Values()
		if err != nil {
			return nil, fmt.Errorf("postgres source: scan: %w", err)
		}
		row, err := dbbatch.EncodeRow(vals)
		if err != nil {
			return nil, fmt.Errorf("postgres source: scan: %w", err)
		}
		batch = append(batch, row)
	}
	// Delayed driver errors land here, not on Next().
	if err := s.rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres source: read: %w", err)
	}
	if s.done && len(batch) == 0 {
		return nil, io.EOF
	}
	return dbbatch.DBBatch{Columns: s.columns, Rows: batch}, nil
}

// Close releases the rows cursor and the connection. Idempotent: Close on an
// uninit Source (no querier) returns nil (matches csv).
func (s *Source) Close() error {
	if s.rows != nil {
		s.rows.Close()
	}
	if s.q != nil {
		s.q.Close()
	}
	return nil
}
