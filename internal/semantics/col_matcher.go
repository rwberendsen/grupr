package semantics

import (
	"fmt"
)

type ColMatcher struct {
	ColExprs ColExprs
}

func newColMatcher(l []string, im InterfaceMetadata) (ColMatcher, error) {
	m := ColMatcher{ColExprs{}}
	for _, expr := range l {
		exprs, err := newColExprs(expr, DTAPs, UserGroups)
		if err != nil {
			return m, fmt.Errorf("parsing column expr: %s", err)
		}
		for e, ea := range exprs {
			if _, ok := m.ColExprs[e]; ok {
				return m, fmt.Errorf("duplicate column expr: '%v', with attributes: '%v'", e, ea)
			}
			m.ColExprs[e] = ea
		}
	}
	if ok := m.ColExprs.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of column exprs")
	}
	for e, ea range m.ColExprs {
		for dtap in ea.DTAPs {
			if im.ObjectMatcher.disjointWithColExpr(e, dtap) {
				return m, fmt.Errorf("column expression '%s' disjoint with object expression", e)
			}
		}
	}
	return m, nil
	// for Validation: if this is not a product-level interface; ObjMatcher part of ColMatcher can not be
	// disjoint with interface ObjMatcher: for each DTAP that is: for each DTAP, there must be an overlap in
	// the objects matched by the ColMatcher and the interface ObjMatcher.
	// If I were to reUse Exprs and friends, I will have to expand the set logic functions with the ability to
	// compare in the context of a particular DTAP. [DONE]
	//
	// Also, for ColMatcher expressions, it's not an error to omit DTAP expansion templates in the object parts.
	// If you use a wild-card in the schema or database or table part, the objects you match will be evaluated for
	// overlap with each respective DTAP anyway, in turn. But, if you need to match a subset of schema's, or tables,
	// then, yes, you may still need to add a DTAP expansion (because wild-cards are only allowed as suffixes, so
	// you can't say *_customer, you'd have to say [dtap]_customer). And you know, you could not spell-out hard-coded
	// DTAPs either, like, prd_customer; cause that particular column matching expression would yield no overlap
	// with object matchers in non production environments. Likewise, you may find you will need to use the
	// [user_group] expansion. The only difference is, wrt to object matchers that are not part of column matching
	// expressions: it is not an error to omit a DTAP expansion.

	// Between themselves, each column matcher must be disjoint with all other columnn matchers of the interface
	// (i.e., a column can only be user group column, masked, or hashed, but not more than one out of three.
	// Two column matchers are disjoint when their object matchers are; or if the expr parts are.
}
