package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjMatcher struct {
	Include        ObjExpr
	ObjExprAttr
	Exclude        map[ObjExpr]bool // No need to store ObjExprAttr of excluded objects
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
	// 2. One of rhs.Exclude[] excludes objects that are not excluded by any of lhs.Exclude[],
	//    while these excluded objects do in fact overlap with lhs.Include ...
	// 	TODO: check that the condition above in the while is fully complete: overlap is still vague: is it subset? And we did not code it yet
	for r := range rhs.Exclude {
		if lhs.Include.subsetOf(r) {
			return false // lhs and rhs are disjoint
		}
		alsoExcludedInLHS := false
		for l := range lhs.Exclude {
			if r.subsetOf(l) {
				alsoExcludedInLHS = true
			}
		}
		if !alsoExcludedInLHS {
			return false // rhs excludes objects that lhs does not exclude, so lhs cannot be a subset
		}
	}
	return true
}

func (lhs ObjMatcher) Equal(rhs ObjMatcher) bool {
	return lhs.Include.Equal(rhs.Include) && lhs.Exclude.Equal(rhs.Exclude)
	// StrictSuperset and StrictSubset are derived from include and exclude, no need to compare
}
