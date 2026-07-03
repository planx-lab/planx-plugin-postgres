package main

import (
	"context"

	"github.com/planx-lab/planx-plugin-postgres/internal/sink"
	"github.com/planx-lab/planx-plugin-postgres/internal/source"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

func main() {
	sdk.Serve(sdk.Plugin{
		ID:          "postgres",
		Version:     "1.0.0",
		DisplayName: "PostgreSQL Connector",
		Description: "Read from and write to PostgreSQL (source + sink).",
		Components: []sdk.ComponentSpec{
			{
				ID: "source", Kind: sdk.KindSource, DisplayName: "PostgreSQL Source", Source: source.New,
				DiscoverSchema: func(ctx context.Context, config []byte) (*sdk.SchemaDiscovery, error) {
					return source.DiscoverSchema(ctx, config, func(cfg source.Config) (source.Querier, error) {
						return source.ConnectQuerier(ctx, cfg)
					})
				},
				ConfigSchema: sdk.Schema(
					sdk.StringField("host", sdk.Required(), sdk.WithDescription("PostgreSQL host")),
					sdk.IntegerField("port", sdk.WithDefault(sdk.IntValue(5432)), sdk.WithDescription("PostgreSQL port")),
					sdk.StringField("database", sdk.Required(), sdk.WithDescription("Database name")),
					sdk.StringField("user", sdk.Required(), sdk.WithDescription("DB user")),
					sdk.SecretField("password", sdk.Required(), sdk.WithDescription("DB password")),
					sdk.StringField("table", sdk.Required(), sdk.WithDescription("Source table (schema.table, e.g. public.users_src)")),
					sdk.StringField("columns", sdk.WithDescription("Columns to read (comma-separated; empty = all)")),
					sdk.IntegerField("batchRows", sdk.WithDefault(sdk.IntValue(1000)), sdk.WithDescription("Rows per batch")),
					sdk.EnumField("sslmode", []string{"disable", "require", "verify-ca", "verify-full"}, sdk.WithDefault(sdk.StringValue("disable")), sdk.WithDescription("SSL mode")),
				),
			},
			{
				ID: "sink", Kind: sdk.KindSink, DisplayName: "PostgreSQL Sink", Sink: sink.New,
				ConfigSchema: sdk.Schema(
					sdk.StringField("host", sdk.Required()),
					sdk.IntegerField("port", sdk.WithDefault(sdk.IntValue(5432))),
					sdk.StringField("database", sdk.Required()),
					sdk.StringField("user", sdk.Required()),
					sdk.SecretField("password", sdk.Required(), sdk.WithDescription("DB password")),
					sdk.StringField("table", sdk.Required(), sdk.WithDescription("Target table (e.g. users or public.users)")),
					sdk.StringField("columns", sdk.WithDescription("Comma-separated column list; if empty, uses batch column schema")),
					sdk.EnumField("sslmode", []string{"disable", "require", "verify-ca", "verify-full"}, sdk.WithDefault(sdk.StringValue("disable"))),
				),
			},
		},
	})
}
