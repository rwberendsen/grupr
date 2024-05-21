package state

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	_ "github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/maps"
)

var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}\*?$`) // lowercase identifier chars + optional wildcard suffix
var validQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)

// currently supported object types
type dbType int

const (
	_database = iota
	_schema
	_table
	_view
)

var dbTypes = [5]string{"DATABASE", "SCHEMA", "TABLE", "VIEW"}
var dbTypeCast = map[string]dbType{
	"DATABASE": _database,
	"SCHEMA":   _schema,
	"TABLE":    _table,
	"VIEW":     _view,
}

var db *sql.DB

func getEnvOrDie(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("env var not found: %s", key)
	}
	return val
}

func init() {
	user := getEnvOrDie("SNOWFLAKE_USER")
	account := getEnvOrDie("SNOWFLAKE_ACCOUNT")
	dbName := getEnvOrDie("SNOWFLAKE_DB")
	params := getEnvOrDie("SNOWFLAKE_CONN_PARAMS")
	var err error
	db, err = sql.Open("snowflake", user+"@"+account+"/"+dbName+params)
	if err != nil {
		log.Fatalf("open db: %s", err)
	}
}


func escapeIdentifier(s string) string {
	return strings.ReplaceAll(s, "\"", "\"\"")
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func createRegexpIdentifier(s string) *regexp.Regexp {
	s = strings.ReplaceAll(s, "$", "\\$") // escape dollar sign, which can be used in Snowflake identifiers
	s = strings.ReplaceAll(s, "*", ".*")  // transform the wildcard suffix into a zero or more regular expression
	s = "(?i)^" + s + "$"                 // match case insensitive; match complete identifier
	return regexp.MustCompile(s)
}

func matchPart(e exprPart, l map[string]bool) map[string]bool {
	r := map[string]bool{}
	if e.is_quoted {
		if _, ok := l[e.s]; ok {
			r[e.s] = true
		}
		return r
	}
	// implement match unquoted with optional suffix wildcard
	// note that we match case insensitive, so `mytable` would match all of
	// "mytable", "MyTable", "MYTABLE", etc.
	re := createRegexpIdentifier(e.s)
	for k, _ := range l {
		if re.MatchString(k) {
			r[k] = true
		}
	}
	return r
}

func match(e expr, c *accountCache) accountObjs {
	o := accountObjs{}
	matchedDBs := matchPart(e[_database], c.getDBnames())
	for db, _ := range matchedDBs {
		o = o.addDB(db, e[_schema].matchAll())
		matchedSchemas := matchPart(e[_schema], c.getDBs()[db].getSchemaNames())
		for schema, _ := range matchedSchemas {
			o = o.addSchema(db, schema, e[_table].matchAll())
			matchedTables := matchPart(e[_table], c.getDBs()[db].getSchemas()[schema].getTableNames())
			matchedViews := matchPart(e[_table], c.getDBs()[db].getSchemas()[schema].getViewNames())
			for t, _ := range matchedTables {
				o = o.addObject(db, schema, t, _table)
			}
			for v, _ := range matchedViews {
				o = o.addObject(db, schema, v, _view)
			}
		}
	}
	return o
}

func querySnowflake(g *grupsDiff) {
	// walk over g, and enrich:
	// - created products and their interfaces with the exprs they consist of
	// - for updated products, both the old and new versions with the objects they consist of
	//
	// for deleted products we don't need to know the objects for now

	// as we match databases and schema's, we build up a local cache of the DB tree.
	c := &accountCache{map[string]*dbCache{}, map[string]bool{}}
	for _, p := range g.created {
		p.matchedInclude = accountObjs{}
		for e, _ := range p.exprs {
			p.matchedInclude = p.matchedInclude.add(match(e, c))
		}
		p.matchedExclude = accountObjs{}
		for e, _ := range p.exprsExclude {
			p.matchedExclude = p.matchedExclude.add(match(e, c))
		}
		p.matched = p.matchedInclude.subtract(p.matchedExclude)
	}
}
