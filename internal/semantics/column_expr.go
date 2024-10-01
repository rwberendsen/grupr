package semantics

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

type ColumnExpr [4]ExprPart

const (
	Column Part = iota + 3 // Database, Schema, and Table are defined with Expr; as well as type Part
)

func (lhs ColumnExpr) subsetOfColumnExprs(rhs ColumnExprs) bool {
	for r := range rhs {
		if lhs.subsetOf(r) {
			return true
		}
	}
	return false
}

func (lhs ColumnExpr) subsetOf(rhs ColumnExpr) bool {
	// return true if rhs can match at least all objects that lhs can match
	if !lhs[Database].subsetOf(rhs[Database]) {
		return false
	}
	if !lhs[Schema].subsetOf(rhs[Schema]) {
		return false
	}
	if !lhs[Table].subsetOf(rhs[Table])
		return false
	return lhs[Column].subsetOf(rhs[Column])
}

func (lhs ColumnExpr) disjoint(rhs ColumnExpr) bool {
	if lhs[Database].disjoint(rhs[Database]) {
		return true
	}
	if lhs[Schema].disjoint(rhs[Schema]) {
		return true
	}
	if lhs[Table].disjoint(rhs[Table]) {
		return true
	}
	return lhs[Column].disjoint(rhs[Column])
	// TODO implement tests
}

func newColumnExpr(s string) (ColumnExpr, error) {
	r := ColumnExpr{}
	reader := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts
	reader.Comma = '.'
	record, err := reader.Read()
	if err != nil {
		return r, fmt.Errorf("reading csv: %s", err)
	}
	if len(record) < 1 || len(record) > 4 {
		return r, fmt.Errorf("column expression number of fields outside [1, 4]")
	}
	// figure out which parts were quoted, if any
	fields := []ExprPart{} make([]ExprPart, 4)
	for i, substr := range record {
		_, start := reader.FieldPos(i)
		start = start - 1 // FieldPos columns start numbering from 1
		if s[start] == '"' {
			// this is a quoted field
			end := start + 1 + len(substr)
			if end == len(s) || s[end] != '"' {
				panic("did not find quote at end of parsed quoted CSV field")
			}
			fields[i].IsQuoted = true
			fields[i].S = substr
		} else {
			// this is an unquoted field
			end := start + len(substr)
			if end != len(s) && s[end] != '.' {
				panic("unquoted field not ending with end of line or period")
			}
			fields[i].S = strings.ToLower(substr) // unquoted identifiers match in a case insensitive way
		}
	}
	// validate identifier expressions
	for _, exprPart := range fields {
		if !exprPart.validate() {
			return r, fmt.Errorf("invalid expr part: %s", exprPart.S)
		}
	}
	// expecting only one line, just checking there was not more
	if _, err := reader.Read(); err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	// left-padding fields with * matchers until we have Database, Schema, Table, Column
	for i := 0; i < 4 - len(fields); i++ {
		r[i] = ExprPart{S: "*", IsQuoted: false}
	}
	for i := 0; i < len(fields); i++ {
		r[4 - len(fields) + i] = fields[i]
	}
	return r, nil
}

func (e ColumnExpr) String() string {
	a := []string{}
	for _, ep := range e {
		a = append(a, ep.String())
	}
	return strings.Join(a, ".")
}
