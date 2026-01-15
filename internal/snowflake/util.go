package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func runSQL(ctx context.Context, cnf *Config, conn *sql.DB, sql string, params ...any) error {
	if cnf.DryRun {
		printSQL(sql, params...)
		return nil
	}
	if _, err := conn.ExecContext(ctx, sql, params...); err != nil { return err }
	return nil
}

func quoteIdentifier(s string) string {
	s = strings.ReplaceAll(`"`, `""`)
	return `"` + s + `"`
}

func printSQL(sql string, params ...any) {
	fmt.Print(sql, "; ")
	for param := range params {
		fmt.Print(", ", param)
	}
	fmt.Print("\n")
}

