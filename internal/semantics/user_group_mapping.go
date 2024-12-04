package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type UserGroupMapping struct {
	ID string `yaml:"id"`
	Mapping map[string]string
}

func newUserGroupMapping(ugmSyn syntax.UserGroupMapping, userGroups UserGroups) (UserGroupMapping, error) {
	ugm := UserGroupMapping{ID: ugmSyn.ID, Mapping: ugmSyn.Mapping}
	for _, v := range ugm.Mapping {
		if _, ok := userGroups[v]; !ok { return ugm, &SetLogicError{fmt.Sprintf("user group mapping '%s': unknown user group '%s'", ugm.ID, v} }
	}
	return ugm, nil
}
