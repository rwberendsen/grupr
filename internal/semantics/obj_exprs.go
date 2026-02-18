package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
)

type objExprs map[ObjExpr]ObjExprAttr

func newObjExprs(cnf *Config, s string, ds DTAPSpec, userGroups map[string]struct{}, userGroupRenderings map[string]syntax.Rendering) (objExprs, error) {
	if len(userGroups) == 0 {
		return newObjExprsWithoutUserGroups(cnf, s, ds)
	}
	return newObjExprsWithUserGroups(cnf, s, ds, userGroups, userGroupRenderings)
}

func newObjExprsWithoutUserGroups(cnf *Config, s string, ds DTAPSpec) (objExprs, error) {
	exprs := objExprs{}
	renderings, err := renderTmplDataDTAP(s, util.Seq2First(ds.All()), ds.DTAPRenderings)
	if err != nil {
		return exprs, err
	}
	for r, m := range renderings {
		if len(m) > 1 {
			return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': multiple associated dtaps", s)}
		}
		expr, err := newObjExpr(cnf, r)
		if err != nil {
			return exprs, err
		}
		for ea := range m {
			exprs[expr] = ea
		}
	}
	return exprs, nil
}

func newObjExprsWithUserGroups(cnf *Config, s string, ds DTAPSpec, userGroups map[string]struct{},
	userGroupRenderings map[string]syntax.Rendering) (objExprs, error) {
	exprs := objExprs{}
	renderings, err := renderTmplDataDTAPUG(s, util.Seq2First(ds.All()), ds.DTAPRenderings, userGroups, userGroupRenderings)
	if err != nil {
		return exprs, err
	}
	for r, m := range renderings {
		var dtap string
		if nDTAPsObjExprAttr(m) > 1 {
			return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': multiple associated dtaps", s)}
		}
		for ea := range m {
			dtap = ea.DTAP
		}

		var ug string
		switch nUGsObjExprAttr(m) {
		case 1:
			for ea := range m {
				ug = ea.UserGroup
			}
		case len(userGroups):
			ug = "" // template did not expand user group, object is shared between usergroups
		default:
			return exprs, &syntax.FormattingError{fmt.Sprintf("'%s': multiple but not all usergroups associated", s)}
		}

		expr, err := newObjExpr(cnf, r)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = ObjExprAttr{DTAP: dtap, UserGroup: ug}
	}
	return exprs, nil
}
