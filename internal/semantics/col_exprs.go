package semantics

import (
	"fmt"
	"maps"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ColExprs map[ColExpr]ColExprAttr

func newColExprs(cnf *Config, s string, dtaps syntax.Rendering, userGroups syntax.Rendering) (ColExprs, error) {
	exprs := ColExprs{}
	if strings.ContainsRune(s, '\n') {
		return exprs, &syntax.FormattingError{"object expression has newline"}
	}
	dtapExpanded := map[string]ColExprAttr{}
	if strings.Contains(s, cnf.DTAPTemplate) {
		// If object exists only in, say, a dev env, that's okay. Cause it's okay if the production rendition of the object does not match any existing objects.
		// What counts is that if they would exist, then they would be matched.
		if len(dtaps) == 0 {
			return exprs, &SetLogicError{fmt.Sprintf("expanding dtaps in '%s': no dtaps found", s)}
		}
		for d, renderedDTAP := range dtaps {
			dtapExpanded[strings.ReplaceAll(s, cnf.DTAPTemplate, renderedDTAP)] = ColExprAttr{DTAP: d}
		}
	} else {
		// In a column matcher expression it is okay to omit a DTAP expansion, the column expressions are evaluated per DTAP,
		// for overlap with the object expressions. So, the column expression is just associated with all DTAPs
		dtapExpanded[s] = ColExprAttr{}
	}
	userGroupExpanded := map[string]ColExprAttr{}
	for k, v := range dtapExpanded {
		if strings.Contains(k, cnf.UserGroupTemplate) {
			// If object only actually exists for, say, one particular user group, that's okay.
			// Cause it's okay if the rendition of the object for other user groups does not match any existing objects.
			// What counts is that if they would exist, then they would be matched.
			if len(userGroups) == 0 {
				return exprs, fmt.Errorf("expanding user groups in '%s': no user groups found", k)
			}
			for u, renderedUserGroup := range userGroups {
				userGroupExpanded[strings.ReplaceAll(k, cnf.UserGroupTemplate, renderedUserGroup)] =
					ColExprAttr{DTAP: v.DTAP, UserGroup: u}
			}
		} else {
			// Objects matched by expression are shared between user groups
			userGroupExpanded[k] = ColExprAttr{DTAP: v.DTAP}
		}
	}
	for k, v := range userGroupExpanded {
		expr, err := newColExpr(k, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = v
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
