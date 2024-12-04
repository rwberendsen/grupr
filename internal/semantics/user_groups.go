package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type UserGroups map[string]bool // true: current; false: historical

func newUserGroups(userGroups syntax.UserGroups) UserGroups {
	ug := UserGroups{}
	for i := range userGroups.Current {
		ug[i] = true // current
	}
	for i := range userGroups.Historical {
		ug[i] = false // false
	}
	return ug
}
