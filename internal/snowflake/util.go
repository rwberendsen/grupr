package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/util"
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
		printMultipleSQL(sql)
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

func printSQL(sql string, params ...any) {
	s := fmt.Sprint(sql, "; ")
	l := util.FmtSliceElements(params...)
	s += fmt.Sprint(strings.Join(l, ", "))
	s += fmt.Sprint("\n")
	fmt.Print(s)
}

func printMultipleSQL(sql string) {
	var s string
	for _, l := range strings.Split(sql, ";") {
		s += fmt.Sprintln(l)
	}
	fmt.Print(s)
}
