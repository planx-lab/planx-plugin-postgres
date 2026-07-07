package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/planx-lab/planx-plugin-postgres/internal/dbcommon"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// DiscoverSchema connects to the DB and introspects information_schema.
// Phase 1 (no table in config): returns the table list.
// Phase 2 (table in config): returns the column list for that table.
//
// DiscoverSchema is a standalone function (not a Source method) so it can take
// a connect callback; main.go wires the real connect (connectQuerier) and tests
// inject a fake querier through the same seam the Source uses.
func DiscoverSchema(ctx context.Context, config []byte, connect func(Config) (querier, error)) (*sdk.SchemaDiscovery, error) {
	var cfg Config
	if len(config) > 0 {
		if err := dbcommon.Parse(string(config), "postgres source", &cfg); err != nil {
			return nil, err
		}
	}
	dbcommon.ApplyDefaults(&cfg.DSNConfig)
	q, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	if cfg.Table == "" {
		return discoverTables(ctx, q)
	}
	schema, name := parseTable(cfg.Table)
	return discoverColumns(ctx, q, schema, name)
}

// discoverTables lists user base tables (excluding system schemas).
// SQL is ANSI information_schema — identical for PG and MSSQL.
func discoverTables(ctx context.Context, q querier) (*sdk.SchemaDiscovery, error) {
	rows, err := q.Query(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		  AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name`)
	if err != nil {
		return nil, fmt.Errorf("postgres source: discover tables: %w", err)
	}
	defer rows.Close()

	var tables []sdk.TableInfo
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("postgres source: discover tables scan: %w", err)
		}
		if len(vals) >= 2 {
			schema, _ := vals[0].(string)
			name, _ := vals[1].(string)
			tables = append(tables, sdk.TableInfo{Schema: schema, Name: name})
		}
	}
	return &sdk.SchemaDiscovery{Tables: tables}, rows.Err()
}

// discoverColumns lists columns of one table.
//
// TODO(ADR-013): schema/table are interpolated, not parameterized — for Alpha
// this is acceptable because the values come from the Designer dropdown (the
// table list) rather than free-text input. A production version should bind
// them as query parameters.
func discoverColumns(ctx context.Context, q querier, schema, table string) (*sdk.SchemaDiscovery, error) {
	rows, err := q.Query(ctx, fmt.Sprintf(`
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = '%s' AND table_name = '%s'
		ORDER BY ordinal_position`, schema, table))
	if err != nil {
		return nil, fmt.Errorf("postgres source: discover columns: %w", err)
	}
	defer rows.Close()

	var columns []sdk.ColumnInfo
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("postgres source: discover columns scan: %w", err)
		}
		if len(vals) >= 3 {
			name, _ := vals[0].(string)
			dataType, _ := vals[1].(string)
			nullableStr, _ := vals[2].(string)
			columns = append(columns, sdk.ColumnInfo{
				Name:     name,
				Type:     dataType,
				Nullable: nullableStr == "YES",
			})
		}
	}
	return &sdk.SchemaDiscovery{Columns: columns}, rows.Err()
}

// parseTable splits "schema.table" into (schema, table). If there is no schema
// prefix, it defaults to "public" (the PG default schema).
func parseTable(qualified string) (string, string) {
	parts := strings.SplitN(qualified, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", qualified
}
