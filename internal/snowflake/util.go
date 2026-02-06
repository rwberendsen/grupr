package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/snowflakedb/gosnowflake"
)

func runSQL(ctx context.Context, cnf *Config, conn *sql.DB, sql string, params ...any) error {
	if cnf.DryRun {
		printSQL(sql, params...)
		return nil
	}
	if _, err := conn.ExecContext(ctx, sql, params...); err != nil {
		if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
			err = ErrObjectNotExistOrAuthorized
		}
		return err
	}
	return nil
}

func runMultipleSQL(ctx context.Context, cnf *Config, conn *sql.DB, sql string, n int) error {
	if cnf.DryRun {
		fmt.Println(sql)
		return nil
	}
	ctx, _ = gosnowflake.WithMultiStatement(ctx, n)
	if _, err := conn.ExecContext(ctx, sql); err != nil {
		if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
			err = ErrObjectNotExistOrAuthorized
		}
		return err
	}
	return nil
}

func quoteIdentifier(s string) string {
	s = strings.ReplaceAll(s, `"`, `""`)
	return `"` + s + `"`
}

func printSQL(sql string, params ...any) {
	fmt.Print(sql, "; ")
	for param := range params {
		fmt.Print(", ", param)
	}
	fmt.Print("\n")
}
