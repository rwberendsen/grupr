package semantics

import (
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ObjExprAttr struct {
	DTAP string `yaml:"dtap,omitempty"`
	// TODO: consider if we really need the renderings here?
	UserGroups syntax.Rendering `yaml:"user_groups,omitempty"`
}

func (lhs ObjExprAttr) Equal(rhs ObjExprAttr) bool {
	return lhs.DTAP == rhs.DTAP && lhs.UserGroups.Equal(rhs.UserGroups)
}
