module tests

go 1.25.2

replace github.com/QuangTung97/dbc v0.0.0 => ../

require (
	github.com/QuangTung97/dbc v0.0.0
	github.com/go-sql-driver/mysql v1.8.1
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.11.2
	github.com/stretchr/testify v1.11.1
)

require (
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
