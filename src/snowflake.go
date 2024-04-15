package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
)

var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_*][a-z0-9_$*]{0,254}$`) // lowercase identifier chars + wildcard expansion
var validQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)

type ObjExpr [3]IdentifierExpr

type ObjExprPart int

const (
	Database ObjExprPart = iota
	Schema
	Object
)

type IdentifierExpr struct {
	s         string
	is_quoted bool
}

func parse_obj_expr(s string) (ObjExpr, error) {
	log.Printf("s: %s", s)
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
		end := start + len(substr)
		if start != 0 && s[start-1] != '.' {
			// this must have been a quoted field
			if s[start-1] != '"' || end == len(s) || s[end] != '"' {
				log.Printf("start: %d", start)
				log.Printf("s[start-1]: %c", s[start-1])
				log.Printf("end: %d", end)
				log.Printf("susbstr: %s", substr)
				panic("did not find start of line, delimiter, or quote at start of parsed CSV field")
			}
			objExpr[i].is_quoted = true
		} else if end != len(s) && s[end] != '.' {
			panic("unquoted field not ending with end of line or period")
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
