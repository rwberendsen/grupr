package semantics

import (
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"slices"
	"strings"
)

type ObjExpr [3]IdentMatcher
type Part int

func newObjExpr(cnf *Config, s string) (ObjExpr, error) {
	r := ObjExpr{}
	reader := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts, except it does not return whether or not a field was quoted
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
		start = start - 1 // FieldPos columns start numbering from 1, "correcting" that here to use common array positioning
		var isQuoted bool
		if s[start] == '"' {
			isQuoted = true
		}
		if im, err := NewIdentMatcher(cnf, substr, isQuoted); err != nil {
			return r, err
		} else {
			r[i] = im
		}
	}
	// expecting only one line, just checking there was not more
	if _, err := reader.Read(); err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	return r, nil
}

func (lhs ObjExpr) subsetOf(rhs ObjExpr) bool {
	// return true if rhs can match at least all objects that lhs can match
	// TODO: figure out how to ensure that we catch error conditions where
	// usergroup / dtap tags might be different for the same objExpr
	if !lhs.Database().subsetOf(rhs.Database()) {
		return false
	}
	if !lhs.Schema().subsetOf(rhs.Schema()) {
		return false
	}
	return lhs.Object().subsetOf(rhs.Object())
}

func (lhs ObjExpr) disjoint(rhs ObjExpr) bool {
	if lhs.Database().disjoint(rhs.Database()) {
		return true
	}
	if lhs.Schema().disjoint(rhs.Schema()) {
		return true
	}
	return lhs.Object().disjoint(rhs.Object())
	// TODO implement tests
	// *.*.*	whatever	!disjoint
	// a.*.*	b.*.*		disjoint
	// a.*.c	a.b.c		!disjoint
	// a.*.c	a.b.d		disjoint
	// ...
}

func (lhs ObjExpr) subsetOfObjExprs(rhs iter.Seq[ObjExpr]) bool {
	for r := range rhs {
		if lhs.subsetOf(r) {
			return true
		}
	}
	return false
}

func allDisjointObjExprs(i iter.Seq[ObjExpr]) error {
	l := slices.Collect(i)
	if len(l) < 2 {
		return nil
	}
	for i := 0; i < len(l)-1; i++ {
		for j := i + 1; j < len(l); j++ {
			if !l[i].disjoint(l[j]) {
				return &SetLogicError{fmt.Sprintf("overlapping ObjExpr's '%s' and '%s'", l[i], l[j])}
			}
		}
	}
	return nil
}

func (e ObjExpr) MatchesAllObjectsInAnySchemaInDB(db Ident) bool {
	if !e.Database().Match(db) {
		return false
	}
	return e.Object().MatchAll()
}

func (e ObjExpr) Database() IdentMatcher {
	return e[0]
}

func (e ObjExpr) Schema() IdentMatcher {
	return e[1]
}

func (e ObjExpr) Object() IdentMatcher {
	return e[2]
}

func (e ObjExpr) String() string {
	a := []string{}
	for _, im := range e {
		a = append(a, im.String())
	}
	return strings.Join(a, ".")
}
