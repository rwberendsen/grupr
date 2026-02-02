package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjMatcher struct {
	Include ObjExpr
	ObjExprAttr
	Exclude  map[ObjExpr]struct{} // No need to store ObjExprAttr of excluded objects
	SubsetOf ObjExpr              // used for any ObjMatcher that is part of an interface; "" means none
}

func (lhs ObjMatcher) disjoint(rhs ObjMatcher) bool {
	if lhs.Include.disjoint(rhs.Include) {
		return true
	}
	for l := range lhs.Exclude {
		if rhs.Include.subsetOf(l) {
			return true
		}
	}
	for r := range rhs.Exclude {
		if lhs.Include.subsetOf(r) {
			return true
		}
	}
	return false
}

func (lhs ObjMatcher) subsetOf(rhs ObjMatcher) bool {
	if !lhs.Include.subsetOf(rhs.Include) {
		return false
	}
	// lhs.Include is subset of rhs.Include

	// Two scenarios are now still possible where lhs is not a subset of rhs though:
	// 1. lhs.Include is also a subset of one of rhs.Exclude[]
	// 2. One of rhs.Exclude[] excludes objects that are in lhs.Include also and that are not excluded by any of lhs.Exclude[],
	for r := range rhs.Exclude {
		if lhs.Include.subsetOf(r) {
			return false // lhs and rhs are disjoint
		}
		if !r.subsetOf(lhs.Include) {
			continue // This rhs exclude is disjoint from lhs.Include, so, not related, let's say
		}
		// r is a subset of lhs.Include.
		// Check that r is also excluded in lhs.Include, otherwise, lhs can not be a subset of rhs
		alsoExcludedInLHS := false
		for l := range lhs.Exclude {
			if r.subsetOf(l) {
				alsoExcludedInLHS = true
			}
		}
		if !alsoExcludedInLHS {
			return false
		}
	}
	return true
}

func (lhs ObjMatcher) validateExprAgainst(rhs ObjMatcher) error {
	// Caller must ensure lhs is subset of rhs
	if lhs.ObjExprAttr != rhs.ObjExprAttr {
		return fmt.Errorf("mismatch in ObjExprAttr")
	}
}

func (lhs ObjMatcher) DisjointFromDB(db string) bool {
	e := ObjExpr{ExprPart{S: db, IsQuoted: true}, ExprPart{S: "*"}, ExprPart{S: "*"}}
	rhs := ObjMatcher{Include: e}
	return lhs.disjoint(rhs)
}

func (lhs ObjMatcher) DisjointFromSchema(db string, schema string) bool {
	e := ObjExpr{ExprPart{S: db, IsQuoted: true}, ExprPart{S: schema, IsQuoted: true}, ExprPart{S: "*"}}
	rhs := ObjMatcher{Include: e}
	return lhs.disjoint(rhs)
}

func (lhs ObjMatcher) DisjointFromObject(db string, schema string, object string) bool {
	e := ObjExpr{ExprPart{S: db, IsQuoted: true}, ExprPart{S: schema, IsQuoted: true}, ExprPart{S: object, IsQuoted: true}}
	rhs := ObjMatcher{Include: e}
	return lhs.disjoint(rhs)
}

func (lhs ObjMatcher) SupersetOfDB(db string) bool {
	e := ObjExpr{ExprPart{S: db, IsQuoted: true}, ExprPart{S: "*"}, ExprPart{S: "*"}}
	rhs := ObjMatcher{Include: e}
	return rhs.subsetOf(lhs)
}

func (lhs ObjMatcher) SupersetOfSchema(db string, schema string) bool {
	e := ObjExpr{ExprPart{S: db, IsQuoted: true}, ExprPart{S: schema, IsQuoted: true}, ExprPart{S: "*"}}
	rhs := ObjMatcher{Include: e}
	return rhs.subsetOf(lhs)
}

func (lhs ObjMatcher) Equal(rhs ObjMatcher) bool {
	return lhs.Include == rhs.Include &&
		lhs.ObjExprAttr == lhs.ObjExprAttr &&
		maps.Equal(lhs.Exclude, rhs.Exclude)
}
