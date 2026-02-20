package syntax

import (
	"fmt"
)

type GlobalUserGroups struct {
	Current    []string
	Historical []string
}

func (u GlobalUserGroups) validate() error {
	m := map[string]bool{}
	for _, i := range append(u.Current, u.Historical...) {
		if _, ok := m[i]; ok {
			return &FormattingError{fmt.Sprintf("user_groups: duplicate user group: '%s'", i)}
		}
		if err := ValidateID(i); err != nil {
			return fmt.Errorf("user_groups: %w", err)
		}
		m[i] = true
	}
	return nil
}
