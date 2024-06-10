package semantics

import (
	"fmt"

	"golang.org/x/exp/maps"
)

type Matcher struct {
	Include  Exprs
	Exclude  Exprs         `yaml:",omitempty"`
	Superset map[Expr]Expr `yaml:",omitempty"`
}

func newMatcher(include []string, exclude []string, DTAPs map[string]KindOfData, UserGroups map[string]bool) (Matcher, error) {
	m := Matcher{Exprs{}, Exprs{}, map[Expr]Expr{}}
	for _, objExpr := range include {
		exprs, err := newExprs(objExpr, DTAPs, UserGroups)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, ea := range exprs {
			if _, ok := m.Include[e]; ok {
				return m, fmt.Errorf("duplicate include expr: '%v', with attributes: '%v'", e, ea)
			}
			m.Include[e] = ea
		}
	}
	if ok := m.Include.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of include exprs")
	}
	for _, objExpr := range exclude {
		exprs, err := newExprs(objExpr, DTAPs, UserGroups)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, ea := range exprs {
			if _, ok := m.Exclude[e]; ok {
				return m, fmt.Errorf("duplicate exclude expr")
			}
			m.Exclude[e] = ea
		}
	}
	if ok := m.Exclude.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of exclude exprs")
	}
	// check that every expr in exclude is a strict subset of exactly one expression in include
	for i := range m.Exclude {
		hasStrictSuperset := 0
		for j := range m.Include {
			if i.subsetOf(j) && !j.subsetOf(i) {
				hasStrictSuperset += 1
				m.Superset[i] = j
			}
		}
		if hasStrictSuperset != 1 {
			return m, fmt.Errorf("exclude expr without exactly one strict superset include expr")
		}
	}
	return m, nil
}

func (lhs Matcher) equals(rhs Matcher) bool {
	return maps.Equal(lhs.Include, rhs.Include) && maps.Equal(lhs.Exclude, rhs.Exclude)
}

func (lhs Matcher) disjoint(rhs Matcher) bool {
	for l := range lhs.Include {
		for r := range rhs.Include {
			if !l.disjoint(r) {
				if !l.subsetOfExprs(rhs.Exclude) && !r.subsetOfExprs(lhs.Exclude) {
					return false
				}
			}
		}
	}
	return true
}
