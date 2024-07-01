package semantics

import (
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
)

type ExprAttr struct {
	DTAP      string `yaml:"dtap,omitempty"`
	DataKind  KindOfData
	UserGroup string `yaml:"user_group,omitempty"`
}
type Exprs map[Expr]ExprAttr

const (
	DTAPTemplate      = "[dtap]"
	UserGroupTemplate = "[user_group]"
)

func newExprs(s string, DTAPs map[string]KindOfData, UserGroups map[string]bool) (Exprs, error) {
	exprs := Exprs{}
	if strings.ContainsRune(s, '\n') {
		return exprs, fmt.Errorf("object expression has newline")
	}
	dtapExpanded := map[string]ExprAttr{}
	if strings.Contains(s, DTAPTemplate) {
		if len(DTAPs) == 0 {
			return exprs, fmt.Errorf("expanding dtaps in '%s': no dtaps found", s)
		}
		for d, kod := range DTAPs {
			dtapExpanded[strings.ReplaceAll(s, DTAPTemplate, d)] = ExprAttr{d, kod, ""}
		}
	} else {
		dtapExpanded[s] = ExprAttr{"", Real, ""} // TODO: also enable user to specify data type at product / interface level for non DTAP expressions
	}
	userGroupExpanded := map[string]ExprAttr{}
	for k, v := range dtapExpanded {
		if strings.Contains(k, UserGroupTemplate) {
			if len(UserGroups) == 0 {
				return exprs, fmt.Errorf("expanding user groups in '%s': no user groups found", k)
			}
			for u := range UserGroups {
				userGroupExpanded[strings.ReplaceAll(k, UserGroupTemplate, u)] = ExprAttr{v.DTAP, v.DataKind, u}
			}
		} else {
			userGroupExpanded[k] = ExprAttr{v.DTAP, v.DataKind, ""}
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
