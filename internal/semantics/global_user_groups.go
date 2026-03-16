package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type GlobalUserGroups map[string]bool // true: current; false: historical

func newGlobalUserGroups(cnf *Config, globalUserGroups *syntax.GlobalUserGroups) (GlobalUserGroups, error) {
	gug := GlobalUserGroups{}
	if globalUserGroups == nil {
		return gug, nil
	}
	for _, i := range globalUserGroups.Current {
		if _, err := NewID(cnf, i); err != nil {
			return gug, fmt.Errorf("global user groups: %w", err)
		}
		if _, ok := gug[i]; ok {
			return gug, fmt.Errorf("duplicate global user group")
		}
		gug[i] = true // current
	}
	for _, i := range globalUserGroups.Historical {
		if _, err := NewID(cnf, i); err != nil {
			return gug, fmt.Errorf("global user groups: %w", err)
		}
		if _, ok := gug[i]; ok {
			return gug, fmt.Errorf("duplicate global user group")
		}
		gug[i] = false // historical
	}
	return gug, nil
}

func (lhs GlobalUserGroups) Equal(rhs GlobalUserGroups) bool {
	return maps.Equal(lhs, rhs)
}
