package main

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
)

var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}\*?$`) // lowercase identifier chars + optional wildcard suffix
var validQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)


// currently supported object types
type dbType int
const (
	database
	schema
	table
	view
)
var dbTypes = [5]string{"DATABASE", "SCHEMA", "TABLE", "VIEW"}
var dbTypeCast = map[string]dbType{
	"DATABASE": database,
	"SCHEMA": schema,
	"TABLE": table,
	"VIEW":  view,
}


// caching objects in Snowflake locally
type accountCache struct {
	dbs map[string]*dbCache
	dbNames map[string]bool
}

type dbCache struct {
	dbName string
	schemas map[string]*schemaCache
	schemaNames map[string]bool
}

type schemaCache struct {
	dbName string
	schemaName string
	// note that if during run time a table is removed, and a view is
	// created with the same name or vice versa then tableNames and
	// viewNames can contain duplicate keys wrt each other
	tableNames map[string]bool
	viewNames map[string]bool
}

func (c *accountCache) addDBs() {
	rows, err := db.Query(`SELECT database_name FROM snowflake.information_schema.databases`)
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var dbName string
		if err = rows.Scan(&dbName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.dbs[dbName] = &dbCache{dbName, map[string]*schemaCache{}, map[string]bool{}}
		c.dbNames[dbName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (c *accountCache) getDBs() map[string]*dbCache {
	if c.dbs == nil {
		c.addDBs()
	}
	return c.dbs
}

func (c *accountCache) getDBnames() map[string]bool {
	if c.dbNames == nil {
		c.addDBs()
	}
	return c.dbNames
}

func (c *dbCache) addSchemas() {
	rows, err := db.Query(fmt.Sprintf(`SELECT schema_name FROM IDENTIFIER('"%s".information_schema.schemata')`, escapeIdentifier(c.dbName)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var schemaName string
		if err = rows.Scan(&schemaName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.schemas[schemaName] = &schemaCache{c.dbName, schemaName, map[string]bool{}, map[string]bool{}}
		c.schemaNames[schemaName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (c *dbCache) getSchemas() map[string]*schemaCache {
	if c.schemas == nil {
		c.addSchemas()
	}
	return c.schemas
}

func (c *dbCache) getSchemaNames() map[string]bool {
	if c.schemaNames == nil {
		c.addSchemas()
	}
	return c.schemaNames
}

func (c *schemaCache) addTables() {
	rows, err := db.Query(fmt.Sprintf(`SELECT table_name FROM "%s".information_schema.tables WHERE table_schema = '%s'`,
					  escapeIdentifier(c.dbName), escapeString(c.schemaName)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.tableNames[tableName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (c *schemaCache) getTableNames() map[string]bool {
	if c.tableNames == nil {
		c.addTables()
	}
	return c.tableNames
}

func (c *schemaCache) addViews() {
	rows, err := db.Query(fmt.Sprintf(`SELECT table_name FROM "%s".information_schema.views WHERE table_schema = '%s'`,
					  escapeIdentifier(c.dbName), escapeString(c.schemaName)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var viewName string
		if err = rows.Scan(&viewName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.tableNames[viewName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (c *schemaCache) getViewNames() map[string]bool {
	if c.viewNames == nil {
		c.addViews()
	}
	return c.viewNames
}

// couple of simple data structures to hold matched objects in account
type accountObjs struct {
	dbs map[string]dbObjs
}

type dbObjs struct {
	schemas map[string]schemaObjs
}

type schemaObjs struct {
	// note that in case of drift during runtime tables and views may
	// contain the same keys (i.e., if during runtime a table was removed
	// and a view with the same name created)
	tables map[string]bool
	views map[string]bool
}

func (o accountObjs) addObject(db string, schema string, obj string, t dbType) accountObjs {
        dbObjs, ok := o.dbs[db]
	if !ok {
		o.dbs[db] = dbObjs{map[string]schemaObjs{}}
	}
	schemaObjs, ok := o.dbs[db].schemas[schema]
	if !ok {
		o.dbs[db].schemas[schema] = schemaObjs{map[string]bool{}, map[string]bool{}}
	}
	if t == table {
		o.dbs[db].schemas[schema].tables[obj] = true
	} else if t == view {
		o.dbs[db].schemas[schema].views[obj] = true
	}
	return o
}

func (lhs accountObjs) subtract(rhs accountObjs) accountObjs {
	res = accountObjs{map[string]dbObjs{}}
	for k, v := range lhs.dbs {
		if v2, ok := rhs.dbs[k]; !ok {
			res.dbs[k] = v
		} else {
			if dbObjs := v.subtract(v2); len(dbObjs) > 0 {
				res.dbs[k] = dbObjs
			}
		}
	}
	return res
}

func (lhs dbObjs) subtract(rhs dbObjs) dbObjs {
	res = dbObjs{map[string]schemaObjs{}}
	for k, v := range lhs.schemas {
		if v2, ok := rhs.schemas[k]; !ok {
			res.schemas[k] = v
		} else {
			if schemaObjs := v.subtract(v2); len(schemaObjs) > 0 {
				res.schemas[k] = schemaObjs
			}
		}
	}
	return res
}

func (lhs schemaObjs) subtract(rhs schemaObjs) schemaObjs {
	res = schemaObjs{map[string]bool{}, map[string]bool{}}
	for k, _ := range lhs.tables {
		if _, ok := rhs.tables[k]; !ok {
			res.tables[k] = true
		}
	}
	for k, _ := range lhs.views {
		if _, ok := rhs.views[k]; !ok {
			res.views[k] = true
		}
	}
	return res
} 

func (lhs accountObjs) add(rhs accountObjs) accountObjs {
	for k, v := range rhs.dbs {
		if v2, ok := lhs.dbs[k]; !ok { lhs.dbs[k] = v } else { lhs.dbs[k] = lhs.dbs[k].add(rhs.dbs[k]) }
	}
	return lhs
}

func (lhs dbObjs) add(rhs dbObjs) dbObjs {
	for k, v := range rhs.schemas {
		if v2, ok := lhs.schemas[k]; !ok { lhs.schemas[k] = v } else { lhs.schemas[k] = lhs.schemas[k].add(rhs.schemas[k]) }
	}
	return lhs
}

func (lhs schemaObjs) add(rhs schemaObjs) schemaObjs {
	for k, _ := range rhs.tables {
		if _, ok := lhs.tables[k]; !ok { lhs.tables[k] = true }
	}
	for k, _ := range rhs.views {
		if _, ok := lhs.views[k]; !ok { lhs.views[k] = true }
	}
}

func (old accountObjs) actions(new accountObjs) actions {
}

// actions holds what to do based on a grupsDiff

type expr [3]exprPart
type exprPart struct {
	s         string
	is_quoted bool
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

func parse_obj_expr(s string) (expr, error) {
	var empty expr // for return statements that have an error
	if strings.ContainsRune(s, '\n') {
		return empty, fmt.Errorf("object expression has newline")
	}
	r := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts
	r.Comma = '.'
	record, err := r.Read()
	if err != nil {
		return empty, fmt.Errorf("reading csv: %s", err)
	}
	if len(record) != 3 {
		return empty, fmt.Errorf("object expression does not have three parts")
	}
	var expr expr
	// figure out which parts were quoted, if any
	for i, substr := range record {
		expr[i].s = substr
		_, start := r.FieldPos(i)
		start = start - 1 // FieldPos columns start numbering from 1
		if s[start] == '"' {
			// this is a quoted field
			end := start + 1 + len(substr)
			if end == len(s) || s[end] != '"' {
				panic("did not find quote at end of parsed quoted CSV field")
			}
			expr[i].is_quoted = true
		} else {
			// this is an unquoted field
			end := start + len(substr)
			if end != len(s) && s[end] != '.' {
				panic("unquoted field not ending with end of line or period")
			}
		}
	}
	// validate identifier expressions
	for _, exprPart := range expr {
		if !exprPart.is_quoted && !validUnquotedExpr.MatchString(exprPart.s) {
			return empty, fmt.Errorf("not a valid unquoted identifier matching expression")
		}
		if exprPart.is_quoted && !validQuotedExpr.MatchString(exprPart.s) {
			return empty, fmt.Errorf("not a valid quoted identifier matching expression")
		}
	}
	// expecting only one line, just checking there was not more
	_, err = r.Read()
	if err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	return expr, nil
}

func escapeIdentifier(s string) string {
	return strings.ReplaceAll(s, "\"", "\"\"")
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func escapeRegExp(s string) string {
	return strings.ReplaceAll(s, "$", "\$")
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
	var validMatchingExpression *regexp.Regexp = regexp.MustCompile("(?i)" + e.s)
	for k, _ := range l {
		if validMatchingExpression.MatchString(l) {
			r[k] = true
		}
	}
	return r
}

func match(e expr, c *accountCache) {
	o := accountObjs{map[string]dbObjs{})
	matchedDBs := matchPart(e[database], c.getDBnames())
	for db, _ := range matchedDBs {
		matchedSchemas := matchPart(e[schema], c.getDBnames()[db].getSchemaNames())
		for schema, _ := range matchedSchemas {
			matchedTables := matchPart(e[table], c.getDBnames()[db].getSchemaNames()[schema].getTableNames())	
			matchedViews := matchPart(e[table], c.getDBnames()[db].getSchemaNames()[schema].getViewNames())	
			for t, _ := range matchedTables { o = o.addObject(db, schema, t, table) }	
			for v, _ := range matchedViews { o = o.addObject(db, schema, v, view) }	
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
		p.matchedInclude = accountObjs{map[string]dbObjs{}}
		for e, _ := range p.exprs {
			p.matchedInclude = p.matchedInclude.add(match(e, c))
		}
		p.matchedExclude = accountObjs{map[string]dbObjs{}}
		for e, _ := range p.exprsExclude {
			p.matchedExclude = p.matchedExclude.add(match(e, c))
		}
		p.matched = p.matchedInclude.subtract(p.matchedExclude)
	}
}

