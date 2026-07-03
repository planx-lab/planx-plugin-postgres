package source

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/planx-lab/planx-sdk-go/sdk"
)

// =============================================================================
// DiscoverSchema — information_schema introspection via the fake querier seam.
// =============================================================================
//
// These tests reuse fakeQuerier/fakeRows from source_test.go (same package).
// The fakeQuerier returns the same canned rows regardless of SQL, which is
// sufficient here: discoverTables/discoverColumns project rows the same way the
// production code does, and DiscoverSchema's dispatch is verified by feeding
// table-less vs table-bearing config into the connect callback.

// fakeConnect returns a connect callback that yields the given fake querier,
// capturing the parsed config so tests can assert what DiscoverSchema parsed.
func fakeConnect(q *fakeQuerier, got *Config) func(Config) (querier, error) {
	return func(cfg Config) (querier, error) {
		*got = cfg
		return q, nil
	}
}

func TestDiscoverTables_ProjectsRowsToTableInfo(t *testing.T) {
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"table_schema", "table_name"},
		data: [][]any{
			{"public", "users_src"},
			{"public", "orders"},
			{"analytics", "events"},
		},
	}}
	var cfg Config
	disc, err := DiscoverSchema(context.Background(), nil, fakeConnect(q, &cfg))
	if err != nil {
		t.Fatalf("DiscoverSchema: %v", err)
	}
	if disc == nil {
		t.Fatal("nil discovery")
	}
	want := []sdk.TableInfo{
		{Schema: "public", Name: "users_src"},
		{Schema: "public", Name: "orders"},
		{Schema: "analytics", Name: "events"},
	}
	if len(disc.Tables) != len(want) {
		t.Fatalf("tables: got %d, want %d (%+v)", len(disc.Tables), len(want), disc.Tables)
	}
	for i, w := range want {
		if disc.Tables[i] != w {
			t.Errorf("table[%d]: got %+v, want %+v", i, disc.Tables[i], w)
		}
	}
	if len(disc.Columns) != 0 {
		t.Errorf("columns should be empty in table-discovery phase, got %d", len(disc.Columns))
	}
}

func TestDiscoverColumns_ProjectsRowsToColumnInfo(t *testing.T) {
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"column_name", "data_type", "is_nullable"},
		data: [][]any{
			{"id", "integer", "NO"},
			{"name", "text", "YES"},
			{"score", "double precision", "YES"},
		},
	}}
	// Table present → column-discovery phase.
	cfg := []byte(`{"host":"h","database":"d","user":"u","password":"p","table":"public.users_src"}`)
	var gotCfg Config
	disc, err := DiscoverSchema(context.Background(), cfg, fakeConnect(q, &gotCfg))
	if err != nil {
		t.Fatalf("DiscoverSchema: %v", err)
	}
	want := []sdk.ColumnInfo{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "name", Type: "text", Nullable: true},
		{Name: "score", Type: "double precision", Nullable: true},
	}
	if len(disc.Columns) != len(want) {
		t.Fatalf("columns: got %d, want %d (%+v)", len(disc.Columns), len(want), disc.Columns)
	}
	for i, w := range want {
		if disc.Columns[i] != w {
			t.Errorf("column[%d]: got %+v, want %+v", i, disc.Columns[i], w)
		}
	}
	if len(disc.Tables) != 0 {
		t.Errorf("tables should be empty in column-discovery phase, got %d", len(disc.Tables))
	}
}

func TestDiscoverSchema_Dispatch_NoTable_CallsDiscoverTables(t *testing.T) {
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"table_schema", "table_name"},
		data: [][]any{{"public", "t1"}},
	}}
	// No table in config → table-discovery phase.
	cfg := []byte(`{"host":"h","database":"d","user":"u","password":"p"}`)
	var gotCfg Config
	disc, err := DiscoverSchema(context.Background(), cfg, fakeConnect(q, &gotCfg))
	if err != nil {
		t.Fatalf("DiscoverSchema: %v", err)
	}
	if len(disc.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(disc.Tables))
	}
	if disc.Tables[0] != (sdk.TableInfo{Schema: "public", Name: "t1"}) {
		t.Errorf("table: got %+v", disc.Tables[0])
	}
	// The parsed config must not have defaulted a table; table stays empty.
	if gotCfg.Table != "" {
		t.Errorf("table should remain empty when absent, got %q", gotCfg.Table)
	}
}

func TestDiscoverSchema_Dispatch_WithTable_CallsDiscoverColumns(t *testing.T) {
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"column_name", "data_type", "is_nullable"},
		data: [][]any{{"c", "text", "YES"}},
	}}
	cfg := []byte(`{"host":"h","database":"d","user":"u","password":"p","table":"public.users_src"}`)
	var gotCfg Config
	disc, err := DiscoverSchema(context.Background(), cfg, fakeConnect(q, &gotCfg))
	if err != nil {
		t.Fatalf("DiscoverSchema: %v", err)
	}
	if len(disc.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(disc.Columns))
	}
	if gotCfg.Table != "public.users_src" {
		t.Errorf("parsed table: got %q, want %q", gotCfg.Table, "public.users_src")
	}
}

func TestParseTable(t *testing.T) {
	cases := []struct {
		in         string
		wantSchema string
		wantTable  string
	}{
		{"public.users_src", "public", "users_src"},
		{"analytics.events", "analytics", "events"},
		{"users_src", "public", "users_src"}, // no schema → default public
		{"a.b.c", "a", "b.c"},                // SplitN limit 2: first dot splits
	}
	for _, c := range cases {
		gotSchema, gotTable := parseTable(c.in)
		if gotSchema != c.wantSchema || gotTable != c.wantTable {
			t.Errorf("parseTable(%q): got (%q,%q), want (%q,%q)", c.in, gotSchema, gotTable, c.wantSchema, c.wantTable)
		}
	}
}

func TestDiscoverSchema_InvalidConfig_WrappedError(t *testing.T) {
	_, err := DiscoverSchema(context.Background(), []byte(`{not json`), fakeConnect(&fakeQuerier{}, &Config{}))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "postgres source: config:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
}

func TestDiscoverSchema_ConnectError_Propagated(t *testing.T) {
	connectErr := errors.New("dial failed")
	_, err := DiscoverSchema(context.Background(), nil, func(Config) (querier, error) {
		return nil, connectErr
	})
	if !errors.Is(err, connectErr) {
		t.Fatalf("connect error not propagated: got %v", err)
	}
}
