package semantics

import (
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
)

type ExprAttr struct {
	DTAP      string `yaml:"dtap,omitempty"`
	UserGroups []string `yaml:"user_groups,omitempty"`
}
type Exprs map[Expr]ExprAttr

const (
	DTAPTemplate      = "[dtap]" // TODO: at some point, make the character(s) used to demarkate this template configurable
	UserGroupTemplate = "[user_group]"
)

func newExprs(s string, DTAPs map[string]string, userGroups map[string]string) (Exprs, error) {
	exprs := Exprs{}
	if strings.ContainsRune(s, '\n') {
		return exprs, fmt.Errorf("object expression has newline")
	}
	dtapExpanded := map[string]ExprAttr{}
	if strings.Contains(s, DTAPTemplate) { // If object exists only in, say, a dev env, that's okay. Cause it's okay if the production rendition of the object does not match any existing objects. What counts is that if they would exist, then they would be matched.
		if len(DTAPs) == 0 {
			return exprs, fmt.Errorf("expanding dtaps in '%s': no dtaps found", s)
		}
		for d, rendered_dtap := range DTAPs {
			dtapExpanded[strings.ReplaceAll(s, DTAPTemplate, rendered_dtap)] = ExprAttr{DTAP: d}
		}
	} else {
		if len(DTAPs) != 0 {
			return exprs, fmt.Errorf("no dtap expr found, but DTAPs are specified")
		}
		dtapExpanded[s] = ExprAttr{}
	}
	userGroupExpanded := map[string]ExprAttr{}
	userGroupKeys := maps.Keys(userGroups)
	for k, v := range dtapExpanded {
		if strings.Contains(k, UserGroupTemplate) { // If object exists only for, say, AYNL, that's okay. Cause it's okay if the rendition of the object for other user groups does not match any existing objects. What counts is that if they would exist, then they would be matched.
			if len(userGroups) == 0 {
				return exprs, fmt.Errorf("expanding user groups in '%s': no user groups found", k)
			}
			for u, rendered_user_group := range userGroups {
				userGroupExpanded[strings.ReplaceAll(k, UserGroupTemplate, rendered_user_group)] = ExprAttr{v.DTAP, []string{u}}
			}
		} else {
			userGroupExpanded[k] = ExprAttr{v.DTAP, userGroupKeys} // Objects matched by expression are shared between user groups
		}
	}
	for k, v := range userGroupExpanded {
		expr, err := newExpr(k)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = v
	}
	return exprs, nil
}

func (m Exprs) allDisjoint() bool {
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
