package semantics

import (
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
)

type ColExprAttr struct {
	DTAPs      map[string]string `yaml:"dtap,omitempty"`
	UserGroups map[string]string `yaml:"user_groups,omitempty"`
}
type ColExprs map[ColExpr]ColExprAttr

const (
	DTAPTemplate      = "[dtap]" // TODO: At some point, make the character(s) used to demarkate this template configurable; or, just the whole template; remember, this expands even inside of quoted parts; it happens before splitting.
	UserGroupTemplate = "[user_group]"
)

func newColExprs(s string, DTAPs map[string]string, userGroups map[string]string) (ColExprs, error) {
	exprs := ColExprs{}
	if strings.ContainsRune(s, '\n') {
		return exprs, fmt.Errorf("object expression has newline")
	}
	dtapExpanded := map[string]ColExprAttr{}
	if strings.Contains(s, DTAPTemplate) { // If object exists only in, say, a dev env, that's okay. Cause it's okay if the production rendition of the object does not match any existing objects. What counts is that if they would exist, then they would be matched.
		if len(DTAPs) == 0 {
			return exprs, fmt.Errorf("expanding dtaps in '%s': no dtaps found", s)
		}
		for d, renderedDTAP := range DTAPs {
			dtapExpanded[strings.ReplaceAll(s, DTAPTemplate, renderedDTAP)] = ColExprAttr{DTAPs: map[string]string{d: renderedDTAP}}
		}
	} else {
		// In a column matcher expression it is okay to omit a DTAP expansion, the column expressions are evaluated per DTAP,
		// for overlap with the object expressions. So, the column expression is just associated with all DTAPs
		dtapExpanded[s] = ColExprAttr{DTAPs: DTAPs}
	}
	userGroupExpanded := map[string]ColExprAttr{}
	for k, v := range dtapExpanded {
		if strings.Contains(k, UserGroupTemplate) { // If object only actually exists for, say, one particular user group, that's okay. Cause it's okay if the rendition of the object for other user groups does not match any existing objects. What counts is that if they would exist, then they would be matched.
			if len(userGroups) == 0 {
				return exprs, fmt.Errorf("expanding user groups in '%s': no user groups found", k)
			}
			for u, renderedUserGroup := range userGroups {
				userGroupExpanded[strings.ReplaceAll(k, UserGroupTemplate, renderedUserGroup)] =
						ColExprAttr{DTAPs: v.DTAPs, UserGroups: map[string]string{u: renderedUserGroup}}
			}
		} else {
			userGroupExpanded[k] = ColExprAttr{DTAPs: v.DTAPs, UserGroups: userGroups} // Objects matched by expression are shared between user groups
		}
	}
	for k, v := range userGroupExpanded {
		expr, err := newColExpr(k)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = v
	}
	return exprs, nil
}

func (m ColExprs) allDisjoint() bool {
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
