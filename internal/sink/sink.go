// Package sink implements the postgres-sink component: bulk INSERT via pgx
// CopyFrom (design §5). It writes through a copyFromPool seam so unit tests
// need no real Postgres (fakePool records rows; no pgxpool in _test.go).
package sink

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/planx-lab/planx-plugin-postgres/internal/dbbatch"
	"github.com/planx-lab/planx-plugin-postgres/internal/dbcommon"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// Config configures the postgres-sink component (design §7 ConfigSchema).
// Shared connection fields live in dbcommon.DSNConfig (embedded); sink adds
// Columns (optional override; if empty, uses batch column schema).
type Config struct {
	dbcommon.DSNConfig
	Columns string `json:"columns"`
}

// Sink bulk-inserts DBBatch rows via pgx CopyFrom (binary COPY protocol).
// CopyFrom is append-only — no ON CONFLICT (design §5 v1 decision).
type Sink struct {
	cfg  Config
	pool copyFromPool
}

// New returns a zero-value Sink satisfying sdk.SinkSPI.
func New() sdk.SinkSPI { return &Sink{} }

// dbbatch gob registration is centralized in the dbbatch package's init()
// (gob.RegisterName under a shared wire name for cross-connector interop).

// Init parses config, validates required fields, applies defaults, builds the
// DSN, connects (pgxpool), and pings. The connection handle is stored as a
// copyFromPool seam so WriteBatch and tests go through the same interface.
func (s *Sink) Init(ctx context.Context, cfg []byte) error {
	if err := dbcommon.Parse(string(cfg), "postgres sink", &s.cfg); err != nil {
		return err
	}
	if err := dbcommon.ValidateCommon(s.cfg.DSNConfig, "postgres sink"); err != nil {
		return err
	}
	dbcommon.ApplyDefaults(&s.cfg.DSNConfig)

	dsn := dbcommon.BuildDSN(s.cfg.DSNConfig)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("postgres sink: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("postgres sink: ping: %w", err)
	}
	s.pool = &pgPool{pool: pool}
	return nil
}

// parseColumns splits a comma-separated column list, trimming whitespace. Empty
// fields are dropped (design §5 columns override). Returns nil for the empty
// string so the caller can fall back to batch.Columns.
func parseColumns(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		if c := strings.TrimSpace(p); c != "" {
			cols = append(cols, c)
		}
	}
	return cols
}

// WriteBatch bulk-inserts a DBBatch via pgx CopyFrom (design §5):
//   - type-assert to dbbatch.DBBatch (clear error on mismatch);
//   - empty batch is a no-op — CopyFrom is NOT called;
//   - columns: config override wins, else batch.Columns;
//   - each row is decoded per Kind via dbbatch.DecodeRowToArgs (typed args for
//     correct INSERT — int64/time.Time/[]byte/nil), then fed to pgx.CopyFromRows;
//   - CopyFrom errors are wrapped with "postgres sink: copy:".
//
// CopyFrom is append-only (no ON CONFLICT) — design §5 v1 decision.
func (s *Sink) WriteBatch(batch sdk.Batch) error {
	dbb, ok := batch.(dbbatch.DBBatch)
	if !ok {
		return fmt.Errorf("postgres sink: expected dbbatch.DBBatch, got %T", batch)
	}
	if len(dbb.Rows) == 0 {
		return nil
	}

	cols := dbb.Columns
	if override := parseColumns(s.cfg.Columns); len(override) > 0 {
		cols = override
	}

	// Build [][]any typed rows for pgx.CopyFromRows. DecodeRowToArgs reads each
	// Kind tag and converts the string slot back to the typed Go value the
	// driver wants as an INSERT param (NULL -> nil, not empty string).
	typed := make([][]any, len(dbb.Rows))
	for i, row := range dbb.Rows {
		args, err := dbbatch.DecodeRowToArgs(row)
		if err != nil {
			return fmt.Errorf("postgres sink: decode: %w", err)
		}
		typed[i] = args
	}

	_, err := s.pool.CopyFrom(context.Background(), pgx.Identifier{s.cfg.Table}, cols, pgx.CopyFromRows(typed))
	if err != nil {
		return fmt.Errorf("postgres sink: copy: %w", err)
	}
	return nil
}

// Close releases the connection pool. Idempotent: Close on an uninit Sink
// (no pool) returns nil (matches csv / source).
func (s *Sink) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}
