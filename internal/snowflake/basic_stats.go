package snowflake

import (
	"fmt"
	"log"
	"strings"

	"github.com/rwberendsen/grupr/internal/config"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type BasicStats struct {
	ProductId             string
	InterfaceId           string
	Expr                  semantics.Expr
	DTAP                  string
	DataKind              semantics.KindOfData
	UserGroup             string
	UserGroupColumnValues []string
	TableCount            int
	ViewCount             int
	ByteCount             int
}

func NewBasicStats(grups semantics.Grups, sfGrups Grups) []*BasicStats {
	r := []*BasicStats{}
	for prdId, prd := range grups.Products {
		for e, ea := range prd.Matcher.Include {
			stats := &BasicStats{
				ProductId:  prdId,
				Expr:       e,
				DTAP:       ea.DTAP,
				UserGroup:  ea.UserGroup,
				TableCount: sfGrups.Products[prdId].Matched.Objects[e].TableCount(),
				ViewCount:  sfGrups.Products[prdId].Matched.Objects[e].ViewCount(),
			}
			r = append(r, stats)
		}
	}
	return r
}

func PersistInSnowflake(stats []*BasicStats) error {
	getValuesSQL := func(stats []*BasicStats) []string {
		r := []string{}
		for _, s := range stats {
			r = append(r, fmt.Sprintf("('%s', '%s', '%v', '%s', '%v', '%s', %d, %d)",
				s.ProductId, s.InterfaceId, s.Expr, s.DTAP, s.DataKind, s.UserGroup, s.TableCount, s.ViewCount))
		}
		return r
	}
	dbName := config.GetEnvOrDie("GRUPR_SNOWFLAKE_DB")
	schema := config.GetEnvOrDie("GRUPR_SNOWFLAKE_SCHEMA")
	gruprPrefix := config.GetEnvOrDefault("GRUPR_SNOWFLAKE_PREFIX", "_grupr")
	sql := fmt.Sprintf(`
CREATE OR REPLACE TABLE %v.%v.%vbasic_stats (
	product_id varchar,
	interface_id varchar,
	expr varchar,
	dtap varchar,
	kind_of_data varchar,
	user_group varchar,
	user_group_column_values array,
	table_count integer,
	view_count integer,
	byte_count integer
) 
`,
		dbName, schema, gruprPrefix)
	log.Printf("sql:\n%s", sql)
	_, err := getDB().Exec(sql)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}
	sql = fmt.Sprintf(`
INSERT INTO %v.%v.%vbasic_stats (
	product_id,
	interface_id,
	expr,
	dtap,
	kind_of_data,
	user_group,
	table_count,
	view_count
)
VALUES
`,
		dbName, schema, gruprPrefix)
	sql += strings.Join(getValuesSQL(stats), "\n")
	log.Printf("sql:\n%s", sql)
	_, err = getDB().Exec(sql)
	if err != nil {
		return fmt.Errorf("insert stats: %v", err)
	}
	return nil
}
