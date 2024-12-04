package syntax

import (
	"fmt"
)

type UserGroups struct {
	Current []string
	Historical []string
}

func (u UserGroups) validate() error {
	m := map[string]bool
	for _, i := range append(u.Current, u.Historical...) {
		if _, ok := m[i] {
			return &FormattingError{fmt.Sprintf("user_groups: duplicate user group: '%s'", i)}
		}
		if err := validateID(i); err != nil { return fmt.Errorf("user_groups: %w", err) }
		m[i] = true
	}
	return nil
}
