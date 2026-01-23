package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjMatchers map[ObjExpr]ObjMatcher

func newObjMatchers(cnf *Config, include []string, exclude []string, dtaps syntax.Rendering, userGroups syntax.Rendering) (ObjMatchers, error) {
	oms := ObjMatchers{}
	// Create ObjMatcher objects for each include expression
	for _, expr := range include {
		objExprs, err := newObjExprs(cnf, expr, dtaps, userGroups)
		if err != nil {
			return oms, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, ea := range objExprs {
			if _, ok := oms[e]; ok {
				return oms, fmt.Errorf("duplicate include expr: '%v', with attributes: '%v'", e, ea)
			}
			oms[e] = ObjMatcher{Include: e, ObjExprAttr: ea,}
		}
	}
	// Check that ObjMatcher objects are all disjoint with regard to each other in the context of this ObjMatchers object;
	// Note that we do not consider exclude expressions here.
	if err := allDisjointObjExprMap(oms); err != nil {
		return oms, err
	}
	// For each rendered exclude expression, assign it to the correct rendered include expression
	for _, expr := range exclude {
		objExprs, err := newObjExprs(expr, dtaps, userGroups)
		if err != nil {
			return oms, fmt.Errorf("parsing obj expr: %s", err)
		}
		for e, _ := range objExprs { // We don't care about DTAP and UserGroup for excluded objects
			hasStrictSuperset := false
			for i, om range oms {
				if e.subsetOf(i) && !i.subsetOf(e) { // e should be a strict subset of exactly one i
					if _, ok := om.Exclude[e]; ok {
						return oms, fmt.Errorf("duplicate exclude expr")
					}
					om.Exlude[e] = struct{}{}
					hasStrictSuperset = true
				}
			}
			if !hasStrictSuperset {
				return oms, fmt.Errorf("orphaned exlude expr")
			}
		}
	}
	// Check, after adding each exclude ObjExpr to the correct include ObjExpr, that each include has disjoint excludes
	for _, objMatcher := range oms {
		if err := allDisjointObjExprMap(oms.Exclude); err != nil {
			return oms, fmt.Errorf("exclude exprs: %w", err)
		}
	}
	return oms, nil
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

func (lhs ObjMatcher) DisjointFromDB(db string) bool {
	for _, om := range oms {
		if !om.DisjointFromDB(db, schema) { return false }
	}
	return true
}

func (oms ObjMatchers) DisjointFromSchema(db string, schema string) bool {
	for _, om := range oms {
		if !om.DisjointFromSchema(db, schema) { return false }
	}
	return true
}

func (oms ObjMatchers) DisjointFromObject(db string, schema string, object string) bool {
	for _, om := range oms {
		if !om.DisjointFromObject(db, schema, object) { return false }
	}
	return true
}

