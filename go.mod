module github.com/planx-lab/planx-plugin-postgres

go 1.25.3

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/planx-lab/planx-sdk-go v0.0.0-00010101000000-000000000000
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/planx-lab/planx-proto v0.0.0-00010101000000-000000000000 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.77.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace github.com/planx-lab/planx-sdk-go => ../planx-sdk-go

replace github.com/planx-lab/planx-proto => ../planx-proto
