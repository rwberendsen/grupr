package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

// Perhaps this need not be a type, but, this file is a handy place for a function checking the keys against the global user groups
type UserGroupMapping map[string]string

func newUserGroupMapping(ugmSyn syntax.UserGroupMapping, globalUserGroups GlobalUserGroups) (UserGroupMapping, error) {
	var ugm UserGroupMapping
	ugm = ugmSyn.Mapping
	for _, v := range ugm {
		if _, ok := globalUserGroups[v]; !ok {
			return ugm, &SetLogicError{fmt.Sprintf("user group mapping '%s': unknown user group '%s'", ugmSyn.ID, v)}
		}
	}
	return ugm, nil
}
