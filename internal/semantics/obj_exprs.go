package semantics

import (
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjExprs map[ObjExpr]ObjExprAttr

const (
	DTAPTemplate      = "[dtap]" // TODO: At some point, make the character(s) used to demarkate this template configurable; or, just the whole template; remember, this expands even inside of quoted parts; it happens before splitting.
	UserGroupTemplate = "[user_group]"
)

func newObjExprs(s string, dtaps syntax.Rendering, userGroups syntax.Rendering) (ObjExprs, error) {
	exprs := ObjExprs{}
	if strings.ContainsRune(s, '\n') {
		return exprs, fmt.Errorf("object expression has newline")
	}
	dtapExpanded := map[string]ObjExprAttr{}
	if strings.Contains(s, DTAPTemplate) { // If object exists only in, say, a dev env, that's okay. Cause it's okay if the production rendition of the object does not match any existing objects. What counts is that if they would exist, then they would be matched.
		if len(dtaps) == 0 {
			return exprs, fmt.Errorf("expanding dtaps in '%s': no dtap renderings found", s)
		}
		for d, renderedDTAP := range dtaps {
			dtapExpanded[strings.ReplaceAll(s, DTAPTemplate, renderedDTAP)] = ObjExprAttr{DTAP: d}
		}
	} else {
		if len(dtaps) != 0 {
			return exprs, fmt.Errorf("The product has dtap renderings, but no DTAP expansion found")
		}
		dtapExpanded[s] = ObjExprAttr{}
	}
	userGroupExpanded := map[string]ObjExprAttr{}
	for k, v := range dtapExpanded {
		if strings.Contains(k, UserGroupTemplate) { // If object only actually exists for, say, one particular user group, that's okay. Cause it's okay if the rendition of the object for other user groups does not match any existing objects. What counts is that if they would exist, then they would be matched.
			if len(userGroups) == 0 {
				return exprs, fmt.Errorf("expanding user groups in '%s': no user groups found", k)
			}
			for u, renderedUserGroup := range userGroups {
				userGroupExpanded[strings.ReplaceAll(k, UserGroupTemplate, renderedUserGroup)] = ObjExprAttr{DTAP: v.DTAP, UserGroups: syntax.Rendering{u: renderedUserGroup}}
			}
		} else {
			userGroupExpanded[k] = ObjExprAttr{DTAP: v.DTAP, UserGroups: userGroups} // Objects matched by expression are shared between user groups
		}
	}
	for k, v := range userGroupExpanded {
		expr, err := newObjExpr(k)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = v
	}
	return exprs, nil
}

func (m ObjExprs) allDisjoint() bool {
	keys := maps.Keys(m)
	if len(keys) < 2 {
		return true
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

func (lhs ObjExprs) Equal(rhs ObjExprs) bool {
	return maps.EqualFunc(lhs, rhs, ObjExprAttr.Equal)
}
