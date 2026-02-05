package syntax

import (
	"fmt"
)

type UserGroupMapping struct {
	ID      string `yaml:"id"`
	Mapping map[string]string
}

func (m UserGroupMapping) validate() error {
	if err := ValidateID(m.ID); err != nil {
		return fmt.Errorf("user_group_mapping: %w", err)
	}
	for k, v := range m.Mapping {
		if err := ValidateID(k); err != nil {
			return fmt.Errorf("user_group_mapping '%s': %w", m.ID, err)
		}
		if err := ValidateID(v); err != nil {
			return fmt.Errorf("user_group_mapping '%s': %w", m.ID, err)
		}
	}
	return nil
}
