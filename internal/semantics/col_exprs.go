package semantics

import (
	"fmt"
	"maps"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ColExprs map[ColExpr]ColExprAttr

func newColExprs(cnf *Config, s string, dtaps map[string]struct{}, userGroups map[string]struct{}, dtapRenderings map[string]syntax.Rendering, userGroupRenderings map[string]syntax.Rendering) (ColExprs, error) {
	if len(userGroups) == 0 {
		return newColExprsWithoutUserGroups(cnf, s, dtaps, dtapRenderings)
	}
	return newColExprsWithUserGroups(cnf, s, dtaps, userGroups, dtapRenderings, userGroupRenderings)
}

func newColExprsWithoutUserGroups(cnf *Config, s string, dtaps map[string]struct{}, dtapRenderings map[string]syntax.Rendering) (ColExprs, error) {
	exprs := objExprs{}
	for dtap := range dtaps {
		data := TmplData{DTAP: dtap, DTAPs: dtapRenderings}
		tmpl, err := template.New("expr").Parse(s)
		if err != nil {
			return exprs, err
		}
		var res strings.Builder
		if err = tmpl.Execute(res, data); err != nil {
			return exprs, err
		}
		expr, err := newColExpr(res, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = ColExprAttr{DTAP: dtap}
	}
	if len(exprs) != len(dtaps) {
		if len(exprs) != 1 {
			return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': number of unique ColExpr objects does not equal number of dtaps and is not one", s)}
		}
		for expr, v := range exprs {
			exprs[expr] = ColExprAttr{DTAP: ""}
		}
		// it is fine to not expand dtaps at all in a col expr, it is checked per dtap if they have overlap with object expressions
	}
	return exprs, nil
}

func newColExprsWithUserGroups(cnf *Config, s string, dtaps map[string]struct{}, userGroups map[string]struct{}, dtapRenderings map[string]syntax.Rendering, userGroupRenderings map[string]syntax.Rendering) (ColExprs, error) {
	exprs := ColExprs{}
	expected := 0
	for dtap := range dtaps {
		dtapExprs := ColExprs{}
		for ug := range userGroups {
			data := TmplDataUG{DTAP: dtap, DTAPs: dtapRenderings, UG: ug, UGs: userGroupRenderings}
			tmpl, err := template.New("expr").Parse(s)
			if err != nil {
				return exprs, err
			}
			var res strings.Builder
			if err = tmpl.Execute(res, data); err != nil {
				return exprs, err
			}
			expr, err := newColExpr(res, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
			dtapExprs[expr] = ColExprAttr{DTAP: dtap, UserGroup: ug}
		}
		if len(dtapExprs) != len(userGroups) {
			if len(dtapExprs) != 1 {
				return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': number of unique ColExpr objects does not equal number of usergroups and is not one", s)}
			}
			expected += 1
			for expr, v := range dtapExprs {
				// Objects matched by this expression are shared between usergroup(s) in interface
				exprs[expr] = ColExprAttr{DTAP: v.DTAP, UserGroup: ""} 
			}
		} else {
			expected += len(userGroups)
			for expr, v := range dtapExprs {
				exprs[expr] = v
			}
		}
	}
	if len(exprs) != expected {
		// WIP TODO there are four possibilities: regarding presence or absence of both dtap and usergroup rendering
		return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': unexpected number of unique ColExpr objects", s)}	
	}
	return exprs, nil
}

func (m ColExprs) allDisjoint() bool {
	if len(m) < 2 {
		return true
	}
	var keys []ColExpr
	for i := range m {
		keys = append(keys, i)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if !keys[i].disjoint(keys[j]) {
				return false
			}
		}
	}
	return true
}

func (lhs ColExprs) Equal(rhs ColExprs) bool {
	return maps.Equal(lhs, rhs)
}
