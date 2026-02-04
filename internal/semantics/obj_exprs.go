package semantics

import (
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type objExprs map[ObjExpr]ObjExprAttr

func newObjExprs(cnf *Config, s string, dtaps syntax.Rendering, userGroups syntax.Rendering) (objExprs, error) {
	exprs := objExprs{}
	if strings.ContainsRune(s, '\n') {
		return exprs, fmt.Errorf("object expression has newline")
	}
	dtapExpanded := map[string]ObjExprAttr{}
	if strings.Contains(s, cnf.DTAPTemplate) { // If object exists only in, say, a dev env, that's okay. Cause it's okay if the production rendition of the object does not match any existing objects. What counts is that if they would exist, then they would be matched.
		if len(dtaps) == 0 {
			return exprs, fmt.Errorf("expanding dtaps in '%s': no dtap renderings found", s)
		}
		for d, renderedDTAP := range dtaps {
			dtapExpanded[strings.ReplaceAll(s, cnf.DTAPTemplate, renderedDTAP)] = ObjExprAttr{DTAP: d}
		}
	} else {
		if len(dtaps) != 0 {
			return exprs, fmt.Errorf("The product has dtap renderings, but no DTAP expansion found")
		}
		dtapExpanded[s] = ObjExprAttr{}
	}
	userGroupExpanded := map[string]ObjExprAttr{}
	for k, v := range dtapExpanded {
		if strings.Contains(k, cnf.UserGroupTemplate) { // If object only actually exists for, say, one particular user group, that's okay. Cause it's okay if the rendition of the object for other user groups does not match any existing objects. What counts is that if they would exist, then they would be matched.
			if len(userGroups) == 0 {
				return exprs, fmt.Errorf("expanding user groups in '%s': no user groups found", k)
			}
			for u, renderedUserGroup := range userGroups {
				userGroupExpanded[strings.ReplaceAll(k, cnf.UserGroupTemplate, renderedUserGroup)] = ObjExprAttr{DTAP: v.DTAP, UserGroup: u}
			}
		} else {
			userGroupExpanded[k] = ObjExprAttr{DTAP: v.DTAP} // Objects matched by expression are considered shared between user groups
		}
	}
	for k, v := range userGroupExpanded {
		expr, err := newObjExpr(k, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = v
	}
	return exprs, nil
}
