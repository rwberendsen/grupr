package semantics

import (
	"fmt"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ColMatcher struct {
	ColExprs ColExprs
}

func newColMatcher(l []string, dtaps syntax.Rendering, userGroups syntax.Rendering, objectMatcher objMatcher) (ColMatcher, error) {
	m := ColMatcher{ColExprs{}}
	for _, expr := range l {
		exprs, err := newColExprs(expr, dtaps, userGroups)
		if err != nil {
			return m,  err
		}
		for e, ea := range exprs {
			if _, ok := m.ColExprs[e]; ok {
				return m, &SetLogicError{fmt.Sprintf("duplicate column expr: '%v'", e)}
			}
			m.ColExprs[e] = ea
		}
	}
	if ok := m.ColExprs.allDisjoint(); !ok {
		return m, &SetLogicError{"non disjoint set of column exprs"}
	}
	for e, ea range m.ColExprs {
		for dtap in ea.DTAPs {
			if objectMatcher.disjointWithColExpr(e, dtap) {
				return m, &SetLogicError{fmt.Sprintf("column expression '%v' disjoint with object matcher", e)}
			}
		}
	}
	return m, nil
}

func (lhs ColMatcher) Equal(rhs Colmatcher) bool {
	return lhs.ColExprs.Equal(rhs.ColExprs)
}
