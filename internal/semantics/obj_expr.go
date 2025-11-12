package semantics

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

type ObjExpr [3]ExprPart
type Part int

const (
	Database Part = iota
	Schema
	Table
)

func newObjExpr(s string) (ObjExpr, error) {
	r := ObjExpr{}
	reader := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts
	reader.Comma = '.'
	record, err := reader.Read()
	if err != nil {
		return r, fmt.Errorf("reading csv: %s", err)
	}
	if len(record) != 3 {
		return r, fmt.Errorf("object expression does not have three parts")
	}
	// figure out which parts were quoted, if any
	for i, substr := range record {
		_, start := reader.FieldPos(i)
		start = start - 1 // FieldPos columns start numbering from 1
		if s[start] == '"' {
			// this is a quoted field
			end := start + 1 + len(substr)
			if end == len(s) || s[end] != '"' {
				panic("did not find quote at end of parsed quoted CSV field")
			}
			r[i].IsQuoted = true
			r[i].S = substr
		} else {
			// this is an unquoted field
			end := start + len(substr)
			if end != len(s) && s[end] != '.' {
				panic("unquoted field not ending with end of line or period")
			}
			r[i].S = strings.ToLower(substr) // unquoted identifiers match in a case insensitive way
		}
	}
	// validate identifier expressions
	for _, exprPart := range r {
		if !exprPart.validate() {
			return r, fmt.Errorf("invalid expr part: %s", exprPart.S)
		}
	}
	// expecting only one line, just checking there was not more
	if _, err := reader.Read(); err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	return r, nil
}

func (lhs ObjExpr) subsetOfObjExprs(rhs ObjExprs) bool {
	for r := range rhs {
		if lhs.subsetOf(r) {
			return true
		}
	}
	return false
}

func (lhs ObjExpr) subsetOf(rhs ObjExpr) bool {
	// return true if rhs can match at least all objects that lhs can match
	// TODO: figure out how to ensure that we catch error conditions where
	// usergroup / dtap tags might be different for the same objExpr
	if !lhs[Database].subsetOf(rhs[Database]) {
		return false
	}
	if !lhs[Schema].subsetOf(rhs[Schema]) {
		return false
	}
	return lhs[Table].subsetOf(rhs[Table])
}

func (lhs ObjExpr) disjoint(rhs ObjExpr) bool {
	if lhs[Database].disjoint(rhs[Database]) {
		return true
	}
	if lhs[Schema].disjoint(rhs[Schema]) {
		return true
	}
	return lhs[Table].disjoint(rhs[Table])
	// TODO implement tests
	// *.*.*	whatever	!disjoint
	// a.*.*	b.*.*		disjoint
	// a.*.c	a.b.c		!disjoint
	// a.*.c	a.b.d		disjoint
	// ...
}

func (e ObjExpr) String() string {
	a := []string{}
	for _, ep := range e {
		a = append(a, ep.String())
	}
	return strings.Join(a, ".")
}
