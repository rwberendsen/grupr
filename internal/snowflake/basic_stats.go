package snowflake

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"golang.org/x/exp/maps"
	"github.com/rwberendsen/grupr/internal/config"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type BasicStats struct {
	ProductId             string
	InterfaceId           string
	ObjExpr               semantics.ObjExpr
	DTAP                  string
	UserGroups            string
	TableCount            int
	ViewCount             int
	ByteCount             int
}

func NewBasicStats(grupin semantics.Grupin, sfGrupin Grupin) []*BasicStats {
	r := []*BasicStats{}
	for prdId, prd := range grupin.Products {
		for e, ea := range prd.ObjectMatcher.Include {
			stats := &BasicStats{
				ProductId:  prdId,
				ObjExpr:    e,
				DTAP:       ea.DTAP,
				TableCount: sfGrupin.Products[prdId].Matched.Objects[e].TableCount(),
				ViewCount:  sfGrupin.Products[prdId].Matched.Objects[e].ViewCount(),
			}
			userGroups := maps.Keys(ea.UserGroups)
			sort.Strings(userGroups)
			stats.UserGroups = strings.Join(userGroups, ",")
			r = append(r, stats)
		}
		for intrfId, intrf := range prd.Interfaces {
			for e, ea := range intrf.ObjectMatcher.Include {
				stats := &BasicStats{
					ProductId:   prdId,
					InterfaceId: intrfId,
					ObjExpr:     e,
					DTAP:        ea.DTAP,
					TableCount:  sfGrupin.Products[prdId].Interfaces[intrfId].Matched.Objects[e].TableCount(),
					ViewCount:   sfGrupin.Products[prdId].Interfaces[intrfId].Matched.Objects[e].ViewCount(),
				}
				userGroups := maps.Keys(ea.UserGroups)
				sort.Strings(userGroups)
				stats.UserGroups = strings.Join(userGroups, ",")
				r = append(r, stats)
			}
		}
	}
	return r
}

func PersistInSnowflake(stats []*BasicStats) error {
	getValuesSQL := func(stats []*BasicStats) []string {
		r := []string{}
		for _, s := range stats {
			r = append(r, fmt.Sprintf("('%s', '%s', '%v', '%s', '%s', %d, %d)",
				s.ProductId, s.InterfaceId, s.ObjExpr, s.DTAP, s.UserGroups, s.TableCount, s.ViewCount))
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
	obj_expr varchar,
	dtap varchar,
	user_groups varchar,
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
	obj_expr,
	dtap,
	user_groups,
	table_count,
	view_count
)
VALUES
`,
		dbName, schema, gruprPrefix)
	sql += strings.Join(getValuesSQL(stats), ",\n")
	log.Printf("sql:\n%s", sql)
	_, err = getDB().Exec(sql)
	if err != nil {
		return fmt.Errorf("insert stats: %v", err)
	}
	return nil
}
