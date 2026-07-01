module github.com/planx-lab/planx-plugin-postgres

go 1.25.3

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/planx-lab/planx-sdk-go v0.0.0-00010101000000-000000000000
)

replace github.com/planx-lab/planx-sdk-go => ../planx-sdk-go

replace github.com/planx-lab/planx-proto => ../planx-proto
