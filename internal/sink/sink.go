// Package sink implements the postgres-sink component: bulk INSERT via pgx
// CopyFrom (design §5). It writes through a copyFromPool seam so unit tests
// need no real Postgres (fakePool records rows; no pgxpool in _test.go).
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/planx-lab/planx-plugin-postgres/internal/dbbatch"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// Config configures the postgres-sink component (design §7 ConfigSchema).
type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	Table    string `json:"table"`
	Columns  string `json:"columns"` // comma-separated; if empty, uses batch column schema
	SSLMode  string `json:"sslmode"`
}

const (
	defaultPort    = 5432
	defaultSSLMode = "disable"
)

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
	if err := parseConfig(string(cfg), &s.cfg); err != nil {
		return err
	}
	if err := validateConfig(&s.cfg); err != nil {
		return err
	}
	applyDefaults(&s.cfg)

	dsn := buildDSN(s.cfg)
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

// parseConfig unmarshals the JSON config, wrapping parse errors with the
// connector/component prefix (design: "<connector> sink: config: %w").
func parseConfig(raw string, cfg *Config) error {
	if err := json.Unmarshal([]byte(raw), cfg); err != nil {
		return fmt.Errorf("postgres sink: config: %w", err)
	}
	return nil
}

// validateConfig enforces the required fields from design §7.
func validateConfig(cfg *Config) error {
	if cfg.Host == "" {
		return fmt.Errorf("postgres sink: host is required")
	}
	if cfg.Database == "" {
		return fmt.Errorf("postgres sink: database is required")
	}
	if cfg.User == "" {
		return fmt.Errorf("postgres sink: user is required")
	}
	if cfg.Password == "" {
		return fmt.Errorf("postgres sink: password is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("postgres sink: table is required")
	}
	return nil
}

// applyDefaults fills port/sslmode when unset (design §7).
func applyDefaults(cfg *Config) {
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = defaultSSLMode
	}
}

// buildDSN constructs the pgx connection URL via net/url so the password is
// URL-encoded safely (url.UserPassword handles special chars). The password is
// never fmt.Printf-ed by this package. Identical to the source's buildDSN.
func buildDSN(cfg Config) string {
	u := url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:   cfg.Database,
	}
	if cfg.User != "" {
		u.User = url.UserPassword(cfg.User, cfg.Password)
	}
	q := u.Query()
	q.Set("sslmode", cfg.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
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
