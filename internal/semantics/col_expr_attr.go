package semantics

import (
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ColExprAttr struct {
	DTAP      string	// Empty means col expr omits DTAP template; it will be evaluated for all DTAPs
	UserGroup string	// Empty means col expr omits UserGroup template; objects will be considered shared
}

func (lhs ColExprAttr) Equal(rhs ColExprAttr) bool {
	return lhs.DTAPs.Equal(rhs.DTAPs) && lhs.UserGroups.Equal(rhs.UserGroups)
}
