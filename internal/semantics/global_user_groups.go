package semantics

import (
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type GlobalUserGroups map[string]bool // true: current; false: historical

func newGlobalUserGroups(globalUserGroups syntax.GlobalUserGroups) GlobalUserGroups {
	gug := GlobalUserGroups{}
	for _, i := range globalUserGroups.Current {
		gug[i] = true // current
	}
	for _, i := range globalUserGroups.Historical {
		gug[i] = false // false
	}
	return gug
}

func (lhs GlobalUserGroups) Equal(rhs GlobalUserGroups) bool {
	return maps.Equal(lhs, rhs)
}
