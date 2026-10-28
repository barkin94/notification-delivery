package bun

import (
	"database/sql"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/uptrace/bun/extra/bunotel"
)

func Connect(databaseURL string) *bun.DB {
	conn := pgdriver.NewConnector(pgdriver.WithDSN(databaseURL))
	sqldb := sql.OpenDB(conn)
	bundb := bun.NewDB(sqldb, pgdialect.New())
	if err := bundb.Ping(); err != nil {
		panic("connect to postgres: " + err.Error())
	}

	// nolint:staticcheck // AddQueryHook is deprecated but WithQueryHook not available in bun v1.3.0
	bundb.AddQueryHook(
		bunotel.NewQueryHook(
			bunotel.WithDBName(conn.Config().Database),
		),
	)

	return bundb
}
