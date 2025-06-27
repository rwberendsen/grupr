package semantics

import (
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ColExprAttr struct {
	// TODO: consider if we really need the renderings here?
	DTAPs      syntax.Rendering `yaml:"dtaps,omitempty"`
	UserGroups syntax.Rendering `yaml:"user_groups,omitempty"`
}

func (lhs ColExprAttr) Equal(rhs ColExprAttr) bool {
	return lhs.DTAPs.Equal(rhs.DTAPs) && lhs.UserGroups.Equal(rhs.UserGroups)
}
