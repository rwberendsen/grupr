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

type ObjExpr [3]IdentifierExpr
type Obj [4]string

type ObjPart int

const (
	Database ObjPart = iota
	Schema
	Object
)

type IdentifierExpr struct {
	s         string
	is_quoted bool
}

// currently supported object types
type ObjType int

const (
	Table ObjType = iota
	View
)

var ObjTypes = [2]string{"TABLE", "VIEW"}
var ObjTypeCast = map[string]ObjType{
	"TABLE": Table,
	"VIEW":  View,
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

func parse_obj_expr(s string) (ObjExpr, error) {
	var empty ObjExpr // for return statements that have an error
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
	var objExpr ObjExpr
	// figure out which parts were quoted, if any
	for i, substr := range record {
		objExpr[i].s = substr
		_, start := r.FieldPos(i)
		start = start - 1 // FieldPos columns start numbering from 1
		if s[start] == '"' {
			// this is a quoted field
			end := start + 1 + len(substr)
			if end == len(s) || s[end] != '"' {
				panic("did not find quote at end of parsed quoted CSV field")
			}
			objExpr[i].is_quoted = true
		} else {
			// this is an unquoted field
			end := start + len(substr)
			if end != len(s) && s[end] != '.' {
				panic("unquoted field not ending with end of line or period")
			}
		}
	}
	// validate identifier expressions
	for _, id_expr := range objExpr {
		if !id_expr.is_quoted && !validUnquotedExpr.MatchString(id_expr.s) {
			return empty, fmt.Errorf("not a valid unquoted identifier matching expression")
		}
		if id_expr.is_quoted && !validQuotedExpr.MatchString(id_expr.s) {
			return empty, fmt.Errorf("not a valid quoted identifier matching expression")
		}
	}
	// expecting only one line, just checking there was not more
	_, err = r.Read()
	if err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	return objExpr, nil
}

func querySnowflake(g *GrupsDiff) {
	// walk over g, and enrich:
	// - created products and their interfaces with the objects they consist of
	// - for updated products, both the old and new versions with the objects they consist of
	//
	// for deleted products we don't need to know the objects for now

	// as we match databases and schema's, we build up a local cache with the objects found so
	// far in Snowflake
	objs := make(map[string]map[string]map[string]bool)
	rows, err := db.Query("SELECT database_name FROM snowflake.information_schema.databases")
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var dbName string
		if err = rows.Scan(&dbName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		objs[dbName] = nil
	}
	log.Printf("objs: %v", objs)
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}
