package semantics

import (
	"fmt"

	"golang.org/x/exp/maps"
)

type Matcher struct {
	Include map[Expr]bool
	Exclude map[Expr]bool
}

func newMatcher(include []string, exclude []string) (Matcher, error) {
	m := Matcher{map[Expr]bool{}, map[Expr]bool{}}
	for _, objExpr := range include {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		if _, ok := m.Include[parsed]; ok {
			return m, fmt.Errorf("duplicate include expr")
		}
		m.Include[parsed] = true
	}
	if ok := m.Include.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of include exprs")
	}
	for _, objExpr := range exclude {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		if _, ok := m.Exclude[parsed]; ok {
			return m, fmt.Errorf("duplicate exclude expr")
		}
		m.Exclude[parsed] = true
	}
	if ok := m.Exclude.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of exclude exprs")
	}
	// check that every expr in exclude is a strict subset of an expression in include
	for i, _ := range m.Exclude {
		hasStrictSuperset := false
		for j, _ := range m.Include {
			if i.subsetOf(j) && !j.isSubsetOf(i) {
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
	return maps.Equals(lhs.Include, rhs.Include) && maps.Equals(lhs.Exclude, rhs.Exclude)
}
