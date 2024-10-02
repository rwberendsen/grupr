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
			return m,  err
		}
		for e, ea := range exprs {
			if _, ok := m.ColExprs[e]; ok {
				return m, SetLogicError{fmt.Sprintf("duplicate column expr: '%v'", e)}
			}
			m.ColExprs[e] = ea
		}
	}
	if ok := m.ColExprs.allDisjoint(); !ok {
		return m, SetLogicError{"non disjoint set of column exprs"}
	}
	for e, ea range m.ColExprs {
		for dtap in ea.DTAPs {
			if im.ObjectMatcher.disjointWithColExpr(e, dtap) {
				return m, SetLogicError{fmt.Sprintf("column expression '%v' disjoint with object matcher", e)}
			}
		}
	}
	return m, nil
}
