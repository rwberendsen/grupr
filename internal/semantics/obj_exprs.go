package semantics

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type objExprs map[ObjExpr]ObjExprAttr

func newObjExprs(cnf *Config, s string, dtaps map[string]struct{}, userGroups map[string]struct{}, dtapRenderings map[string]syntax.Rendering, userGroupRenderings map[string]syntax.Rendering) (objExprs, error) {
	if len(userGroups) == 0 {
		return newObjExprsWithoutUserGroups(cnf, s, dtaps, dtapRenderings)
	}
	return newObjExprsWithUserGroups(cnf, s, dtaps, userGroups, dtapRenderings, userGroupRenderings)
}

func newObjExprsWithoutUserGroups(cnf *Config, s string, dtaps map[string]struct{}, dtapRenderings map[string]syntax.Rendering) (objExprs, error) {
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
		expr, err := newObjExpr(res, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = ObjExprAttr{DTAP: dtap}
	}
	if len(exprs) != len(dtaps) {
		return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': number of unique ObjExpr objects does not equal number of dtaps", s)}	
	}
	return exprs, nil
}

func newObjExprsWithUserGroups(cnf *Config, s string, dtaps map[string]struct{}, userGroups map[string]struct{}, dtapRenderings map[string]syntax.Rendering, userGroupRenderings map[string]syntax.Rendering) (objExprs, error) {
	exprs := objExprs{}
	expected := 0
	for dtap := range dtaps {
		dtapExprs := objExprs{}
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
			expr, err := newObjExpr(res, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
			dtapExprs[expr] = ObjExprAttr{DTAP: dtap, UserGroup: ug}
		}
		if len(dtapExprs) != len(userGroups) {
			if len(dtapExprs) != 1 {
				return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': number of unique ObjExpr objects does not equal number of usergroups and is not one", s)}
			}
			expected += 1
			for expr, v := range dtapExprs {
				// Objects matched by this expression are shared between usergroup(s) in interface
				exprs[expr] = ObjExprAttr{DTAP: v.DTAP, UserGroup: ""} 
			}
		} else {
			expected += len(userGroups)
			for expr, v := range dtapExprs {
				exprs[expr] = v
			}
		}
	}
	if len(exprs) != expected {
		return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': unexpected number of unique ObjExpr objects", s)}	
	}
	return exprs, nil
}
