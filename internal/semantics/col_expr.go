package semantics

import (
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"maps"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ColExpr [4]IdentMatcher

const (
	Column Part = iota + 3 // Database, Schema, and Object are defined with Expr; as well as type Part
)

func newColExpr(cnf *Config, s string) (ColExpr, error) {
	r := ColExpr{}
	reader := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts
	reader.Comma = '.'
	record, err := reader.Read()
	if err != nil {
		return r, fmt.Errorf("reading csv: %w", err)
	}
	if len(record) < 1 || len(record) > 4 {
		return r, &syntax.FormattingError{"column expression number of fields outside [1, 4]"}
	}
	// figure out which parts were quoted, if any
	fields := []IdentMatcher{}
	for i, substr := range record {
		_, start := reader.FieldPos(i)
		start = start - 1 // FieldPos columns start numbering from 1
		var isQuoted bool
		if s[start] == '"' {
			isQuoted = true
		}
		if im, err := NewIdentMatcher(cnf, substr, isQuoted); err != nil {
			return r, err
		} else {
			fields = append(fields, im)
		}
	}
	// expecting only one line, just checking there was not more
	if _, err := reader.Read(); err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	// left-padding fields with * matchers until we have Database, Schema, Object, Column
	for i := 0; i < 4-len(fields); i++ {
		r[i] = NewMatchallIdentMatcher()
	}
	for i := 0; i < len(fields); i++ {
		r[4-len(fields)+i] = fields[i]
	}
	return r, nil
}

func (lhs ColExpr) subsetOf(rhs ColExpr) bool {
	// return true if rhs can match at least all objects that lhs can match
	if !lhs.Database().subsetOf(rhs.Database()) {
		return false
	}
	if !lhs.Schema().subsetOf(rhs.Schema()) {
		return false
	}
	if !lhs.Object().subsetOf(rhs.Object()) {
		return false
	}
	return lhs.Column().subsetOf(rhs.Column())
}

func (lhs ColExpr) disjoint(rhs ColExpr) bool {
	if lhs.Database().disjoint(rhs.Database()) {
		return true
	}
	if lhs.Schema().disjoint(rhs.Schema()) {
		return true
	}
	if lhs.Object().disjoint(rhs.Object()) {
		return true
	}
	return lhs.Column().disjoint(rhs.Column())
	// TODO implement tests
}

func (c ColExpr) subsetOfObjExprs(objExprs iter.Seq[ObjExpr]) bool {
	objExpr := ObjExpr{c[0], c[1], c[2]}
	return objExpr.subsetOfObjExprs(objExprs)
}

func (c ColExpr) disjointWithObjExpr(e ObjExpr) bool {
	objExpr := ObjExpr{c[0], c[1], c[2]}
	return objExpr.disjoint(e)
}

func (c ColExpr) disjointWithObjMatchers(oms ObjMatchers, dtap string) bool {
	for _, om := range oms {
		if om.DTAP == dtap {
			if !c.disjointWithObjExpr(om.Include) {
				if !c.subsetOfObjExprs(maps.Keys(om.Exclude)) {
					return false
				}
			}
		}
	}
	return true
}

func (e ColExpr) Database() IdentMatcher {
	return e[0]
}

func (e ColExpr) Schema() IdentMatcher {
	return e[1]
}

func (e ColExpr) Object() IdentMatcher {
	return e[2]
}

func (e ColExpr) Column() IdentMatcher {
	return e[3]
}

func (e ColExpr) String() string {
	a := []string{}
	for _, im := range e {
		a = append(a, im.String())
	}
	return strings.Join(a, ".")
}
