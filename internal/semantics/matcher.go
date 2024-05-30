package semantics

import (
	"fmt"

	"golang.org/x/exp/maps"
)

type Matcher struct {
	Include Exprs
	Exclude Exprs
}

func newMatcher(include []string, exclude []string) (Matcher, error) {
	m := Matcher{Exprs{}, Exprs{}}
	for _, objExpr := range include {
		exprs, err := parseObjExpr(objExpr)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e := range exprs {
			if _, ok := m.Include[e]; ok {
				return m, fmt.Errorf("duplicate include expr")
			}
			m.Include[e] = true
		}
	}
	if ok := m.Include.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of include exprs")
	}
	for _, objExpr := range exclude {
		exprs, err := parseObjExpr(objExpr)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e := range exprs {
			if _, ok := m.Exclude[e]; ok {
				return m, fmt.Errorf("duplicate exclude expr")
			}
			m.Exclude[e] = true
		}
	}
	if ok := m.Exclude.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of exclude exprs")
	}
	// check that every expr in exclude is a strict subset of an expression in include
	for i, _ := range m.Exclude {
		hasStrictSuperset := false
		for j, _ := range m.Include {
			if i.subsetOf(j) && !j.subsetOf(i) {
				hasStrictSuperset = true
			}
		}
		if !hasStrictSuperset {
			return m, fmt.Errorf("exclude expr without strict superset include expr")
		}
	}
	return m, nil
}

func (lhs Matcher) equals(rhs Matcher) bool {
	return maps.Equal(lhs.Include, rhs.Include) && maps.Equal(lhs.Exclude, rhs.Exclude)
}

func (lhs Matcher) disjoint(rhs Matcher) bool {
	for l, _ := range lhs.Include {
		for r, _ := range rhs.Include {
			if !l.disjoint(r) {
				if !l.subsetOfExprs(rhs.Exclude) && !r.subsetOfExprs(lhs.Exclude) {
					return false
				}
			}
		}
	}
	return true
}
