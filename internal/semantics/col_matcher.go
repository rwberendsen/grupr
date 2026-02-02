package semantics

import (
	"fmt"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ColMatcher struct {
	ColExprs ColExprs
}

func newColMatcher(cnf *Config, l []string, dtaps syntax.Rendering, userGroups syntax.Rendering, objectMatchers ObjMatchers) (ColMatcher, error) {
	m := ColMatcher{ColExprs{}}
	for _, expr := range l {
		exprs, err := newColExprs(cnf, expr, dtaps, userGroups)
		if err != nil {
			return m, err
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
	for e, ea := range m.ColExprs {
		dtapsToCheck := map[string]bool{}
		if ea.DTAP == "" {
			for dtap := range dtaps {
				dtapsToCheck[dtap] = true
			}
		} else {
			dtapsToCheck[ea.DTAP] = true
		}
		for dtap, _ := range dtapsToCheck {
			if e.disjointWithObjMatchers(objectMatchers, dtap) {
				return m, &SetLogicError{fmt.Sprintf("column expression '%v' disjoint with object matcher", e)}
			}
		}
	}
	return m, nil
}

func (lhs ColMatcher) Equal(rhs ColMatcher) bool {
	return lhs.ColExprs.Equal(rhs.ColExprs)
}
