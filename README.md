# Planx PostgreSQL Connector

A Planx 4.0 connector that reads from and writes to PostgreSQL. One
self-describing binary exposing two components (ADR-009):
- **Source** (`source`): executes a finite SELECT query via pgx and streams rows
  in batches, EOF at end-of-result. Type-tagged via the `DBBatch` envelope for
  full fidelity across int64/float64/string/bool/time/[]byte/NULL.
- **Sink** (`sink`): bulk-inserts incoming batches via pgx `CopyFrom` (binary
  COPY protocol). Append-only in v1.

## Build

```bash
go build -o plugin ./cmd/plugin
```

## Run (via Planx Engine)

Place the `plugin` binary at `<pluginsRoot>/postgres/plugin` and start the
engine; it Discovers the connector automatically (ADR-008, no manifest).

## Config

### Source component
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | STRING | yes | — | PostgreSQL host |
| `port` | INTEGER | no | 5432 | PostgreSQL port |
| `database` | STRING | yes | — | Database name |
| `user` | STRING | yes | — | DB user |
| `password` | SECRET | yes | — | DB password (masked, never logged) |
| `query` | STRING | yes | — | SELECT query (finite result set) |
| `batchRows` | INTEGER | no | 1000 | Rows per batch |
| `sslmode` | ENUM | no | `disable` | One of `disable`, `require`, `verify-ca`, `verify-full` |

### Sink component
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | STRING | yes | — | PostgreSQL host |
| `port` | INTEGER | no | 5432 | PostgreSQL port |
| `database` | STRING | yes | — | Database name |
| `user` | STRING | yes | — | DB user |
| `password` | SECRET | yes | — | DB password (masked, never logged) |
| `table` | STRING | yes | — | Target table (e.g. `users` or `public.users`) |
| `columns` | STRING | no | — | Comma-separated column list; if empty, uses batch column schema |
| `sslmode` | ENUM | no | `disable` | One of `disable`, `require`, `verify-ca`, `verify-full` |

The Sink is append-only in v1 (CopyFrom path; no upsert/`ON CONFLICT`).

## Specification Authority
The authoritative spec lives in
[planx-spec](https://github.com/planx-lab/planx-spec) — see
[`db-connectors-design.md`](https://github.com/planx-lab/planx-spec/blob/main/db-connectors-design.md).
