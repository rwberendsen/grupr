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

var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_*][a-z0-9_$*]{0,254}$`) // lowercase identifier chars + wildcard expansion
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
type node struct {
	chld map[dbObj]*node
}
type dbObj struct {
	db string
	schema string
	name string
	dbType dbType
	// by the time we start supporting stored procedures we'd have to come up with
	// a comparabe data structure that captures a variable amount of argument types
	// cause Snowflake supports stored procedure overloading; probably just add a string
	// with types separated by semicolons or something
}

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

func addDBs(node *node) {
	rows, err := db.Query(`SELECT database_name FROM snowflake.information_schema.databases`)
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	node.chld = make(map[dbObj]node)
	for rows.Next() {
		var dbName string
		if err = rows.Scan(&dbName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		node.chld[dbObj{dbName, "", "", database}] = node{nil}
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func addSchemas(node *node, parent dbObj) {
	rows, err := db.Query(fmt.Sprintf(`SELECT schema_name FROM IDENTIFIER('"%s".information_schema.schemata')`, escapeQuotes(parent.db)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	node.chld = make(map[dbObj]node)
	for rows.Next() {
		var schemaName string
		if err = rows.Scan(&schemaName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		node.chld[dbObj{parent.db, schemaName, "", schema}] = node{nil}
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func addTables(node *node, parent dbObj) {
	rows, err := db.Query(fmt.Sprintf(`SELECT table_name FROM "%s".information_schema.tables WHERE table_schema = '%s'`, escapeIdentifier(parent.db), escapeString(parent.schema)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	if node.chld == nil {
		node.chld = make(map[dbObj]node)
	}
	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		node.chld[dbObj{parent.db, parent.schema, tableName, table}] = node{nil}
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func addViews(node *node, parent dbObj) {
	rows, err := db.Query(fmt.Sprintf(`SELECT table_name FROM "%s".information_schema.views WHERE table_schema = '%s'`, escapeIdentifier(parent.db), escapeString(parent.schema)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	if node.chld == nil {
		node.chld = make(map[dbObj]node)
	}
	for rows.Next() {
		var viewName string
		if err = rows.Scan(&viewName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		node.chld[dbObj{parent.db, parent.schema, viewName, view}] = node{nil}
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func matchUnquoted(s string, l map[string]bool) map[string]bool {
	return l
}

func match(e expr, accountNode *node) map[dbObj]bool {
	m := make(map[dbObj]bool)
	var matchedDBs map[string]bool
	if e[database].is_quoted {
		_, ok := accountNode.chld[dbObj{e[database].s, "", "", database}]
		if ok {
			matchedDBs = map[string]bool{e[database].s: true,}
		}
	} else {
		cachedDBs := make(map[string]bool)
		for k, _ := range accountNode.chld {
			cachedDBs[k.db] = true
		}
		matchedDBs = matchUnquoted(e[database], cachedDBs) 
	}
	for db, _ := range matchedDBs {
		dbObj := dbObj{db, "", "", database}
		dbNode := accountNode.chld[dbObj]
		if dbNode.chld == nil {
			addSchemas(dbNode, dbObj)
		}
		var matchedSchemas map[string]bool
		if e[schema].is_quoted {
			_, ok := dbNode.chld[dbObj{db, e[schema].s, "", schema}]
			if ok {
				matchedSchemas = map[string]bool{e[schema].s: true]
			}
		} else {
			cachedSchemas := make(map[string]bool)
			for k, _ := range dbNode.chld {
				cachedSchemas[k.schema] = true
			}
			matchedSchemas = matchUnquoted(e[schema], cachedSchemas)
		}
	}
	return m
}

func querySnowflake(g *grupsDiff) {
	// walk over g, and enrich:
	// - created products and their interfaces with the exprs they consist of
	// - for updated products, both the old and new versions with the objects they consist of
	//
	// for deleted products we don't need to know the objects for now

	// as we match databases and schema's, we build up a local cache of the DB tree.
	var accountNode := node{nil}
	addDBs(&accountNode)
	for _, p := range g.created {
		p.matched = make(map[dbObj]bool)
		for e, _ := range p.exprs {
			for o, _ :=  range match(e, &accountNode) {
				p.matched[o] = true
			}
		}
		p.matchedExclude = make(map[dbObj]bool)
		for e, _ := range p.objectsExclude {
			for o, _ :=  range match(e, &accountNode) {
				p.matchedExclude[o] = true
			}
		}
	}
}

