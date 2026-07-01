package sink

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/planx-lab/planx-plugin-postgres/internal/dbbatch"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// --- fakePool: the DB-free seam (design §8) --------------------------------
//
// Injects a recording copyFromPool so unit tests never touch a real Postgres.
// Lives in the same package so it can satisfy the unexported copyFromPool
// interface. The fake records every row it would have inserted, letting tests
// assert typed args (int64/time.Time/[]byte/nil), the column list actually
// passed to CopyFrom, and that CopyFrom was/was-not called (empty-batch no-op).
//
// grep pgxpool in _test.go -> nothing (real DB never reached). pgx IS imported
// here, but only for pgx.Identifier / pgx.CopyFromSource — the wire types the
// production Sink passes through the seam, NOT a live connection.

// rowReceived is one row the fake would have inserted, captured as the typed
// []any the production Sink derived via dbbatch.DecodeRowToArgs.
type rowReceived struct {
	args []any
}

// fakePool is a copyFromPool that records CopyFrom invocations.
type fakePool struct {
	// recorded inputs:
	table   pgx.Identifier
	columns []string
	rows    []rowReceived

	// behavior knobs:
	copyErr error // if non-nil, returned from CopyFrom instead of recording
	closed  bool
}

func (p *fakePool) CopyFrom(_ context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	p.table = tableName
	p.columns = columnNames
	if p.copyErr != nil {
		// still drain the source so call-count semantics match production
		for rowSrc.Next() {
		}
		return 0, p.copyErr
	}
	for rowSrc.Next() {
		vals, err := rowSrc.Values()
		if err != nil {
			return 0, err
		}
		// copy so later rows don't alias the same backing array
		cp := make([]any, len(vals))
		copy(cp, vals)
		p.rows = append(p.rows, rowReceived{args: cp})
	}
	return int64(len(p.rows)), rowSrc.Err()
}

func (p *fakePool) Close() { p.closed = true }

// withPool is the test-only injection point for the seam.
func withPool(p copyFromPool) func(*Sink) {
	return func(s *Sink) { s.pool = p }
}

// newSinkWithPool wires a Sink whose config is already parsed and whose pool is
// the fake — skipping the real DSN/connect/Ping path entirely.
func newSinkWithPool(cfg Config, p copyFromPool) *Sink {
	return &Sink{cfg: cfg, pool: p}
}

// =============================================================================
// 1. Compile-time SPI conformance
// =============================================================================

func TestSink_New_ReturnsSinkSPI(t *testing.T) {
	var _ sdk.SinkSPI = New()
}

// =============================================================================
// 2. Init config-parse (parse + validate + defaults — DB-free)
// =============================================================================

func TestSink_Init_ParsesValidConfig(t *testing.T) {
	cfg := Config{}
	raw := `{"host":"db","port":6543,"database":"shop","user":"u","password":"p","table":"users","columns":"id,name","sslmode":"require"}`
	if err := parseConfig(raw, &cfg); err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "db" || cfg.Port != 6543 || cfg.Database != "shop" ||
		cfg.User != "u" || cfg.Password != "p" || cfg.Table != "users" ||
		cfg.Columns != "id,name" || cfg.SSLMode != "require" {
		t.Fatalf("parsed config mismatch: %+v", cfg)
	}
}

func TestSink_Init_InvalidJSON_WrappedError(t *testing.T) {
	var cfg Config
	err := parseConfig(`{not json`, &cfg)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	// Must be wrapped with the "postgres sink: config:" prefix (design §8).
	if !strings.HasPrefix(err.Error(), "postgres sink: config:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
}

func TestSink_Init_MissingRequired(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string // substring of the missing field
	}{
		{"host", `{"database":"d","user":"u","password":"p","table":"t"}`, "host"},
		{"database", `{"host":"h","user":"u","password":"p","table":"t"}`, "database"},
		{"user", `{"host":"h","database":"d","password":"p","table":"t"}`, "user"},
		{"password", `{"host":"h","database":"d","user":"u","table":"t"}`, "password"},
		{"table", `{"host":"h","database":"d","user":"u","password":"p"}`, "table"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cfg Config
			if err := parseConfig(c.json, &cfg); err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			if err := validateConfig(&cfg); err == nil {
				t.Fatalf("expected error for missing %s", c.want)
			} else if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("missing %s: got %q", c.want, err.Error())
			}
		})
	}
}

func TestSink_Init_DefaultsApplied(t *testing.T) {
	var cfg Config
	raw := `{"host":"h","database":"d","user":"u","password":"p","table":"t"}`
	if err := parseConfig(raw, &cfg); err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig: %v", err)
	}
	applyDefaults(&cfg)
	if cfg.Port != 5432 {
		t.Errorf("port default: got %d, want 5432", cfg.Port)
	}
	if cfg.SSLMode != "disable" {
		t.Errorf("sslmode default: got %q, want %q", cfg.SSLMode, "disable")
	}
}

// =============================================================================
// 3. WriteBatch via fakePool (design §8 sink tests)
// =============================================================================

// (a) empty batch is a NO-OP — CopyFrom must NOT be called.
func TestSink_WriteBatch_EmptyBatch_NoOp(t *testing.T) {
	p := &fakePool{}
	s := newSinkWithPool(Config{Table: "users"}, p)

	err := s.WriteBatch(dbbatch.DBBatch{Columns: []string{"id"}, Rows: nil})
	if err != nil {
		t.Fatalf("WriteBatch empty: %v", err)
	}
	if len(p.rows) != 0 {
		t.Fatalf("CopyFrom called on empty batch: recorded %d rows", len(p.rows))
	}
	if p.columns != nil {
		t.Fatalf("CopyFrom invoked on empty batch: columns=%v", p.columns)
	}
}

// (b) type-assertion failure returns a clear error.
func TestSink_WriteBatch_TypeAssertionFailure(t *testing.T) {
	p := &fakePool{}
	s := newSinkWithPool(Config{Table: "users"}, p)

	err := s.WriteBatch([][]string{{"a", "b"}}) // wrong type — not dbbatch.DBBatch
	if err == nil {
		t.Fatal("expected type-assertion error, got nil")
	}
	if !strings.Contains(err.Error(), "expected dbbatch.DBBatch") {
		t.Fatalf("error: got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "[][]string") {
		t.Fatalf("error should name the bad type %%T: got %q", err.Error())
	}
}

// (c) valid batch — fakePool receives the rows with correctly typed args
// decoded per Kind via dbbatch.DecodeRowToArgs (int64/time.Time/[]byte/nil).
func TestSink_WriteBatch_ValidBatch_TypedArgs(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	row, err := dbbatch.EncodeRow([]any{
		int64(42),
		float64(3.5),
		"hello",
		true,
		ts,
		[]byte{0xDE, 0xAD, 0xBE, 0xEF},
		nil,
	})
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}

	p := &fakePool{}
	s := newSinkWithPool(Config{Table: "users"}, p)

	batch := dbbatch.DBBatch{
		Columns: []string{"i", "f", "s", "b", "t", "by", "n"},
		Rows:    []dbbatch.DBRow{row},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if len(p.rows) != 1 {
		t.Fatalf("rows received: got %d, want 1", len(p.rows))
	}
	args := p.rows[0].args
	if len(args) != 7 {
		t.Fatalf("args width: got %d, want 7", len(args))
	}

	// int64
	if v, ok := args[0].(int64); !ok || v != 42 {
		t.Errorf("arg 0: got %#v, want int64(42)", args[0])
	}
	// float64
	if v, ok := args[1].(float64); !ok || v != 3.5 {
		t.Errorf("arg 1: got %#v, want float64(3.5)", args[1])
	}
	// string
	if v, ok := args[2].(string); !ok || v != "hello" {
		t.Errorf("arg 2: got %#v, want string hello", args[2])
	}
	// bool
	if v, ok := args[3].(bool); !ok || !v {
		t.Errorf("arg 3: got %#v, want bool true", args[3])
	}
	// time.Time
	if v, ok := args[4].(time.Time); !ok || !v.Equal(ts) {
		t.Errorf("arg 4: got %#v, want %v", args[4], ts)
	}
	// []byte
	if v, ok := args[5].([]byte); !ok {
		t.Errorf("arg 5: got %T, want []byte", args[5])
	} else if len(v) != 4 || v[0] != 0xDE || v[3] != 0xEF {
		t.Errorf("arg 5 bytes: got % x, want deadbeef", v)
	}

	// table identifier passed through
	if len(p.table) != 1 || p.table[0] != "users" {
		t.Errorf("table identifier: got %v, want [users]", p.table)
	}
	// columns came from batch (no config override here)
	wantCols := []string{"i", "f", "s", "b", "t", "by", "n"}
	if len(p.columns) != len(wantCols) {
		t.Fatalf("columns width: got %d, want %d", len(p.columns), len(wantCols))
	}
	for i, c := range wantCols {
		if p.columns[i] != c {
			t.Errorf("col %d: got %q, want %q", i, p.columns[i], c)
		}
	}
}

// (d) columns override from config takes precedence over batch.Columns.
func TestSink_WriteBatch_ColumnsOverrideFromConfig(t *testing.T) {
	row, err := dbbatch.EncodeRow([]any{int64(1), "alice"})
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	p := &fakePool{}
	// Config.Columns = "user_id,name" must WIN over batch.Columns ["id","nm"].
	s := newSinkWithPool(Config{Table: "users", Columns: "user_id,name"}, p)

	batch := dbbatch.DBBatch{
		Columns: []string{"id", "nm"},
		Rows:    []dbbatch.DBRow{row},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	want := []string{"user_id", "name"}
	if len(p.columns) != len(want) {
		t.Fatalf("override columns width: got %d, want %d", len(p.columns), len(want))
	}
	for i, c := range want {
		if p.columns[i] != c {
			t.Errorf("override col %d: got %q, want %q", i, p.columns[i], c)
		}
	}
}

// (e) NULL slot decodes to a nil arg, NOT an empty string.
func TestSink_WriteBatch_NullSlotBecomesNilArg(t *testing.T) {
	row, err := dbbatch.EncodeRow([]any{nil, "x"})
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	p := &fakePool{}
	s := newSinkWithPool(Config{Table: "t"}, p)

	if err := s.WriteBatch(dbbatch.DBBatch{
		Columns: []string{"a", "b"},
		Rows:    []dbbatch.DBRow{row},
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(p.rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(p.rows))
	}
	if p.rows[0].args[0] != nil {
		t.Errorf("NULL arg: got %#v, want nil — must not be empty string", p.rows[0].args[0])
	}
}

// (f) CopyFrom error from the pool is wrapped.
func TestSink_WriteBatch_CopyFromError_Wrapped(t *testing.T) {
	copyErr := errors.New("connection refused")
	p := &fakePool{copyErr: copyErr}
	s := newSinkWithPool(Config{Table: "t"}, p)

	row, err := dbbatch.EncodeRow([]any{int64(1)})
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	err = s.WriteBatch(dbbatch.DBBatch{
		Columns: []string{"id"},
		Rows:    []dbbatch.DBRow{row},
	})
	if err == nil {
		t.Fatal("expected wrapped CopyFrom error")
	}
	if !strings.HasPrefix(err.Error(), "postgres sink: copy:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
	if !errors.Is(err, copyErr) {
		t.Fatalf("error not wrapping original: got %q", err.Error())
	}
}

// =============================================================================
// 4. Close
// =============================================================================

func TestSink_Close_Uninit_NilError(t *testing.T) {
	s := New()
	if err := s.Close(); err != nil {
		t.Fatalf("Close uninit: %v", err)
	}
}

func TestSink_Close_AfterInit_CallsPoolClose(t *testing.T) {
	p := &fakePool{}
	s := &Sink{pool: p}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after init: %v", err)
	}
	if !p.closed {
		t.Fatal("pool.Close was not called")
	}
}

// Close is idempotent: calling twice does not panic or double-close the
// underlying pool in a way that surfaces an error to the caller.
func TestSink_Close_Idempotent(t *testing.T) {
	p := &fakePool{}
	s := &Sink{pool: p}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
}
