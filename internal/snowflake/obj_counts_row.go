package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
)

type ObjCountsRow struct {
	ProductID   string
	InterfaceID string
	DTAP        string
	UserGroups  string
	TableCount  int
	ViewCount   int
}

func StoreObjCountsRows(ctx context.Context, cnf *Config, conn *sql.DB, rows iter.Seq[ObjCountsRow]) error {
	var productIDs []string
	var interfaceIDs []string
	var dtaps []string
	var userGroups []string
	var tableCounts []string
	var viewCounts []string

	for r := range rows {
		productIDs = append(productIDs, r.ProductID)
		interfaceIDs = append(interfaceIDs, r.InterfaceID)
		dtaps = append(dtaps, r.DTAP)
		userGroups = append(userGroups, r.UserGroups)
		tableCounts = append(tableCounts, r.TableCount)
		viewCounts = append(viewCounts, r.ViewCount)
	}

	sql := fmt.Sprintf(`
CREATE OR REPLACE TABLE %v.%v.%vbasic_stats (
	product_id varchar,
	dtap varchar,
	interface_id varchar,
	user_groups varchar,
	table_count integer,
	view_count integer,
)
`,
		cnf.Database, cnf.Schema, cnf.Prefix)
	if err := runSQL(ctx, cnf, conn, sql); err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	sql = fmt.Sprintf(`
INSERT INTO %v.%v.%vobject_counts (
	product_id,
	dtap,
	interface_id,
	user_groups,
	table_count,
	view_count
)
VALUES (?, ?, ?, ?, ?, ?)
`,
		cnf.Database, cnf.Schema, cnf.Prefix)
	if err := runSQL(ctx, cnf, conn, sql, productIDs, interfaceIDs, dtaps, userGroups, tableCounts, viewCounts); err != nil {
		return fmt.Errorf("insert stats: %v", err)
	}
	return nil
}
