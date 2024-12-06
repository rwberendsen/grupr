package syntax

import (
	"fmt"
)

type UserGroupMapping struct {
	ID string `yaml:"id"`
	Mapping map[string]string
}

func (m UserGroupMapping) validate() error {
	if err := validateID(m.ID); err != nil { return fmt.Errorf("user_group_mapping: %w", err) }
	values := map[string]bool{}
	for k, v := range m.Mapping {
		if err := validateID(k); err != nil { return fmt.Errorf("user_group_mapping '%s': %w", m.ID, err) }
		if err := validateID(v); err != nil { return fmt.Errorf("user_group_mapping '%s': %w", m.ID, err) }
		if _, ok := values[v]; ok { return &FormattingError{fmt.Sprintf("user_group_mapping '%s': duplicate user group: '%s'", m.ID, v)} }
		values[v] = true
	}
	return nil
}
