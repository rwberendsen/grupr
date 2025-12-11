package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjMatchers map[ObjExpr]ObjMatcher

func newObjMatchers(cnf *Config, include []string, exclude []string, dtaps syntax.Rendering, userGroups syntax.Rendering) (ObjMatchers, error) {
	m := map[ObjExpr]ObjMatcher{}
	// Create ObjMatcher objects for each include expression
	for _, expr := range include {
		objExprs, err := newObjExprs(cnf, expr, dtaps, userGroups)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, ea := range objExprs {
			if _, ok := m.Include[e]; ok {
				return m, fmt.Errorf("duplicate include expr: '%v', with attributes: '%v'", e, ea)
			}
			m[e] = ObjMatcher{Include: e, ObjExprAttr: ea}
		}
	}
	// Check that ObjMatcher objects are all disjoint with regard to each other in the context of this ObjMatchers object;
	// Note that we do not consider exclude expressions here.
	if err := allDisjointObjExprMap(m); err != nil {
		return m, err
	}
	// For each rendered exclude expression, assign it to the correct rendered include expression
	for _, expr := range exclude {
		objExprs, err := newObjExprs(expr, dtaps, userGroups)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, _ := range objExprs { // We don't care about DTAP and UserGroup for excluded objects
			hasStrictSuperset := false
			for i, _ range m {
				if e.subsetOf(i) && !i.subsetOf(e) { // e should be a strict subset of exactly one i
					if _, ok := i.Exclude[e]; ok {
						return m, fmt.Errorf("duplicate exclude expr")
					}
					i.Exlude[e] = true
					hasStrictSuperset = true
				}
			}
			if !hasStrictSuperset {
				return m, fmt.Errorf("orphaned exlude expr")
			}
			m.Exclude[e] = ea
		}
	}
	// Check, after adding each exclude ObjExpr to the correct include ObjExpr, that each include has disjoint excludes
	for _, objMatcher := range m {
		if err := allDisjointObjExprMap(m.Exclude); err != nil {
			return m, fmt.Errorf("exclude exprs: %w", err)
		}
	}
	return m, nil
}

func (lhs ObjMatchers) validateExprAttrAgainst(rhs ObjMatchers) error {
	// Caller must ensure lhs is a subset of rhs
	for _, om_lhs := range lhs {
		for _, om_rhs := range rhs {
			if om_lhs.subsetOf(om.rhs) {
				if err := om_lhs.validateExprAttrAgainst(om_rhs); err != nil {
					return err
				}
			}
			break
		}
	}
	return nil
}

func (lhs ObjMatchers) Equal(rhs ObjMatchers) bool {
	return maps.EqualFunc(lhs, rhs, ObjMatcher.Equal)
}

func (lhs ObjMatchers) disjoint(rhs ObjMatchers) bool {
	for _, l := range lhs {
		for _, r := range rhs {
			if !l.disjoint(r) {
				return false
			}
		}
	}
	return true
}

func (lhs ObjMatchers) subsetOf(rhs ObjMatchers) bool {
	for _, l := range lhs {
		hasSuperSet := false
		for _, r := range rhs {
			if l.subsetOf(r) {
				hasSuperSet = true
				break
			}
		}
		if !hasSuperSet {
			return false
		}
	}
	return true
}

func (lhs ObjMatchers) setSubsetOf(rhs ObjMatchers) ObjMatchers {
	ret := ObjMatchers{}
	for eLHS, omLHS := range lhs {
		for eRHS, omRHS := range rhs {
			if omLHS.subsetOf(omRHS) {
				omLHS.SubsetOf = eRHS
				break
			}
		}
		ret[eLHS] = omLHS
	}
	return ret	
}
