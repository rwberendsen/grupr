package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjMatchers struct {
	ObjectMatchers map[ObjExpr]ObjMatcher
}

type ObjMatchers map[ObjExpr]ObjMatcher

func newObjMatcher(include []string, exclude []string, dtaps syntax.Rendering, userGroups syntax.Rendering) (ObjMatcher, error) {
	m := ObjMatcher{ObjExprs{}, ObjExprs{}, map[ObjExpr]ObjExpr{}, map[ObjExpr]ObjExpr{}}
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

func (lhs ObjMatcher) disjoint(rhs ObjMatcher) bool {
	for l := range lhs.Include {
		for r := range rhs.Include {
			if !l.disjoint(r) {
				if !l.subsetOfObjExprs(rhs.Exclude) && !r.subsetOfObjExprs(lhs.Exclude) {
					return false
				}
			}
		}
	}
	return true
}

func (lhs ObjMatcher) subsetOf(rhs ObjMatcher) bool {
	// TODO: This logic is not fully correct, to be correct it should at least consider also lhs.Exclude
	//
	// 	 Also: this logic fails to catch edge cases where the rendering of dtap or usergroup was mixed up, e.g,
	// 	 group_a_[user_group] and [user_group]_group_a render as the same expression when user_group is group_a,
	//       but they are different before rendering.
	//	 worse:
	//       group_a_[usergroup] with usergroup being group_b and [usergroup]_group_b with usergroup begin group_a
	// 	 would label the same expression with a different usergroup
	//
	//	 to fix these subtle issues, a principled approach would be best:
	//	 
	//	 - our little set algebra should be evaluated before rendering usergroups and dtaps (!)
	//	 - an objectMatcher should just refer to a single object matching expression, and it should have its
 	// 	   exclude expressions; after all, an exclude expression can only be a subset of a single include
	//	   expression, since all sibling include expressions are disjoint.
	// 	
	// 	 yes: this would be quite a large code change, but I believe it is a necessary one to do immediately
	//       it will simplify the code and logic considerably. And the project is still in that early stage
	// 	 (version 0) where we can make large changes without much consideration for end users. On top of
	//	 that, this change does not lead to a change in the YAML format.
	for l := range lhs.Include {
		hasSuperset := false
		for r := range rhs.Include {
			if l.subsetOf(r) {
				if rExclude, ok := rhs.StrictSubset[r]; !ok {
					hasSuperset = true
					break
				} else {
					if !l.subsetOf(rExclude) {
						hasSuperset = true
						break
					}
				}
			}
		}
		if !hasSuperset {
			return false
		}
	}
	return true
}
