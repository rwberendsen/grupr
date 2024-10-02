package semantics

import (
	"fmt"

	"golang.org/x/exp/maps"
)

type ObjMatcher struct {
	Include  ObjExprs
	Exclude  ObjExprs         `yaml:",omitempty"`
	Superset map[ObjExpr]ObjExpr `yaml:",omitempty"`
}

func newObjMatcher(include []string, exclude []string, im InterfaceMetadata) (ObjMatcher, error) {
	m := ObjMatcher{Exprs{}, Exprs{}, map[ObjExpr]ObjExpr{}}
	for _, expr := range include {
		objExprs, err := newObjExprs(expr, DTAPs, UserGroups)
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
		objExprs, err := newObjExprs(expr, DTAPs, UserGroups, false)
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
				m.Superset[i] = j
			}
		}
		if hasStrictSuperset != 1 {
			return m, fmt.Errorf("exclude expr without exactly one strict superset include expr")
		}
	}
	return m, nil
}

func (lhs ObjMatcher) equals(rhs ObjMatcher) bool {
	return maps.Equal(lhs.Include, rhs.Include) && maps.Equal(lhs.Exclude, rhs.Exclude)
}

func (lhs ObjMatcher) disjoint(rhs ObjMatcher) bool {
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

func (om ObjMatcher) disjointWithColumnExpr(c ColExpr, dtap string) {
	for o, attr := range om.Include {
		if attr.DTAP == dtap {
			if !c.disjointWithExpr(o) {
				if !c.subsetOfObjExprs(om.Exclude) {
					return false
				}
			}
		}
	}
	return true
}
