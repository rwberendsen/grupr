package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjMatcher struct {
	Include  ObjExprs
	Exclude  ObjExprs         `yaml:",omitempty"`
	StrictSuperset map[ObjExpr]ObjExpr `yaml:"strict_superset,omitempty"` // value is strict superset of key
	StrictSubset map[ObjExpr]ObjExpr `yaml:"strict_subset,omitempty"` // value is strict subset of key
}

func newObjMatcher(include []string, exclude []string, dtaps syntax.Rendering, userGroups syntax.Rendering) (ObjMatcher, error) {
	m := ObjMatcher{ObjExprs{}, ObjExprs{}, map[ObjExpr]ObjExpr{}, map[ObjExpr]ObjExpr{}}
	for _, expr := range include {
		objExprs, err := newObjExprs(expr, dtaps, userGroups)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, ea := range objExprs {
			if _, ok := m.Include[e]; ok {
				return m, fmt.Errorf("duplicate include expr: '%v', with attributes: '%v'", e, ea)
			}
			m.Include[e] = ea
		}
	}
	if ok := m.Include.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of include exprs")
	}
	for _, expr := range exclude {
		objExprs, err := newObjExprs(expr, dtaps, userGroups)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, ea := range objExprs {
			if _, ok := m.Exclude[e]; ok {
				return m, fmt.Errorf("duplicate exclude expr")
			}
			m.Exclude[e] = ea
		}
	}
	if ok := m.Exclude.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of exclude exprs")
	}
	// Check that every expr in exclude is a strict subset of exactly one expression in include
	for i := range m.Exclude {
		hasStrictSuperset := 0
		for j := range m.Include {
			if i.subsetOf(j) && !j.subsetOf(i) {
				hasStrictSuperset += 1
				m.StrictSuperset[i] = j
				m.StrictSubset[j] = i
			}
		}
		if hasStrictSuperset != 1 {
			return m, fmt.Errorf("exclude expr without exactly one strict superset include expr")
		}
	}
	return m, nil
}

func (lhs ObjMatcher) Equal(rhs ObjMatcher) bool {
	return lhs.Include.Equal(rhs.Include) && lhs.Exclude.Equal(rhs.Exclude)
	// StrictSuperset and StrictSubset are derived from include and exclude, no need to compare
}

func (lhs ObjMatcher) disjoint(rhs ObjMatcher) bool {
	for l := range lhs.Include {
		for r := range rhs.Include {
			if !l.disjoint(r) {
				if !l.subsetOfObjExprs(rhs.Exclude) && !r.subsetOfObjExprs(lhs.Exclude) {
					return false
				}
			}
		}
	}
	return true
}

func (lhs ObjMatcher) subsetOf(rhs ObjMatcher) bool {
	for l := range lhs.Include {
		hasSuperset := false
		for r := range rhs.Include {
			if l.subsetOf(r) {
				if rExclude, ok := rhs.StrictSubset[r]; ok {
					if !l.subsetOf(rExclude) {
						hasSuperset = true
						break
					}
				}
			}
		}
		if !hasSuperset { return false }
	}
	return true
}
