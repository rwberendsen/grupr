package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjMatchers struct {
	ObjectMatchers map[ObjExpr]ObjMatcher
}

type ObjMatchers map[ObjExpr]ObjMatcher

func newObjMatchers(cnf *Config, include []string, exclude []string) (ObjMatchers, error) {
	m := map[ObjExpr]ObjMatcher{}
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

func (m ObjMatchers) allDisjoint() error {
	if len(m) < 2 {
		return nil
	}
	var keys []ObjExpr
	for k := range m {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if !m[keys[i]].disjoint(m[keys[j]]) {
				return &SetLogicError{fmt.Sprintf("overlapping ObjMatcher's '%s' and '%s'", keys[i], keys[j])}
			}
		}
	}
	return nil
}

func (lhs ObjMatchers) disjoint(rhs ObjMatchers) bool {
	return !lhs.subsetOf(rhs) && !rhs.subsetOf(lhs)
}

func (lhs ObjMatchers) subsetOf(rhs ObjMatchers) bool {
	for _, l := range lhs {
		hasSuperSet := false
		for _, r := range rhs {
			if l.subsetOf(r) {
				hasSuperSet = true
			}
		}
		if !hasSuperSet {
			return false
		}
	}
}
