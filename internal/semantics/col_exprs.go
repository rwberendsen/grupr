package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
)

type ColExprs map[ColExpr]ObjExprAttr

func newColExprs(cnf *Config, s string, ds DTAPSpec, userGroups map[string]struct{}, userGroupRenderings map[string]syntax.Rendering) (ColExprs, error) {
	if len(userGroups) == 0 {
		return newColExprsWithoutUserGroups(cnf, s, ds)
	}
	return newColExprsWithUserGroups(cnf, s, ds, userGroups, userGroupRenderings)
}

func newColExprsWithoutUserGroups(cnf *Config, s string, ds DTAPSpec) (ColExprs, error) {
	exprs := ColExprs{}
	renderings, err := renderTmplDataDTAP(s, util.Seq2First(ds.All()), ds.DTAPRenderings)
	if  err != nil {
		return exprs, err
	}
	for r, m := range renderings {
		var dtap string
		switch nDTAPsObjExprAttr(m) {
		case 1:
			for ea := range m {
				dtap = ea.DTAP
			}
		case ds.Count():
			dtap = "" // template did not expand dtap, col expr not associated with any particular DTAP
		default:
			return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': multiple associated dtaps, but not all")}
		}
	
		expr, err := newColExpr(r, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = ObjExprAttr{DTAP: dtap}
	}
	return exprs, nil
}

func newColExprsWithUserGroups(cnf *Config, s string, ds DTAPSpec, userGroups map[string]struct{},
	userGroupRenderings map[string]syntax.Rendering) (ColExprs, error) {
	exprs := ColExprs{}
	renderings, err := renderTmplDataDTAPUG(s, util.Seq2First(ds.All()), ds.DTAPRenderings, userGroups, userGroupRenderings)
	if err != nil {
		return exprs, err
	}
	for r, m := range renderings {
		var dtap string
		switch nDTAPsObjExprAttr(m) {
		case 1:
			for ea := range m {
				dtap = ea.DTAP
				break
			}
		case ds.Count():
			dtap = "" // template did not expand dtap, col expr not associated with any particular DTAP
		default:
			return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': multiple associated dtaps, but not all")}
		}

		var ug string
		switch nUGsObjExprAttr(m) {
		case 1: 
			for ea := range m {
				ug = ea.UserGroup
				break
			}
		case len(userGroups): 
			ug = "" // template did not expand usergroup, object is shared between usergroups; or, col expr overlaps with multiple obj exprs from different usergroups, this is allowed
		default:
			return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': multiple but not all usergroups associated")}
		}

		expr, err := newColExpr(r, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = ObjExprAttr{DTAP: dtap, UserGroup: ug}
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
