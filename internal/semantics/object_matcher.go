package semantics

import (
	"fmt"

	"golang.org/x/exp/maps"
)

type ObjectMatcher struct {
	Include  Exprs
	Exclude  Exprs         `yaml:",omitempty"`
	Superset map[Expr]Expr `yaml:",omitempty"`
}

func newObjectMatcher(include []string, exclude []string, im InterfaceMetadata, isPartOfColumnMatcher bool) (ObjectMatcher, error) {
	if isPartOfColumnMatcher && exclude != nil {
		panic("Column matching expressions do not come with the option to exclude objects")
	}
	m := ObjectMatcher{Exprs{}, Exprs{}, map[Expr]Expr{}}
	for _, objExpr := range include {
		exprs, err := newExprs(objExpr, DTAPs, UserGroups, isPartOfColumnMatcher)
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
	if ok := m.Include.allDisjoint(isPartOfColumnMatcher); !ok {
		return m, fmt.Errorf("non disjoint set of include exprs")
	}
	for _, objExpr := range exclude {
		exprs, err := newExprs(objExpr, DTAPs, UserGroups, false)
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
	if ok := m.Exclude.allDisjoint(false); !ok {
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

func (lhs ObjectMatcher) equals(rhs ObjectMatcher) bool {
	return maps.Equal(lhs.Include, rhs.Include) && maps.Equal(lhs.Exclude, rhs.Exclude)
}

func (lhs ObjectMatcher) disjoint(rhs ObjectMatcher) bool {
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

func (lhs ObjectMatcher) disjointInDTAP(rhs ObjectMatcher, DTAP string) {
	for lk, lv := range lhs.Include {
		if _, lOK := lv.DTAPs[DTAP]; lOK {
			for rk, rv := range rhs.Include {
				if _, rOK := rv.DTAPs[DTAP]; rOK {
					if !lk.disjoint(rk) {
						if !lk.subsetOfExprs(rhs.Exclude) && !rk.subsetOfExprs(lhs.Exclude) {
							return false
						}
					}
				}
			}
		}
	}
	return true
}
