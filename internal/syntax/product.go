package syntax

import (
	"fmt"
)

type Product struct {
	ID                  string            `yaml:"id"`
	DTAPs               DTAPSpec          `yaml:"dtaps,flow,omitempty"`
	Consumes            []ConsumptionSpec `yaml:"consumes",omitempty"`
	InterfaceMetadata   `yaml:",inline"`
	DTAPRenderings      map[string]Rendering `yaml:"dtap_renderings,omitempty"`
	UserGroupMappingID  string               `yaml:"user_group_mapping,omitempty"`
	UserGroupRenderings map[string]Rendering `yaml:"user_group_renderings,omitempty"`
	UserGroupColumn     string               `yaml:"user_group_column,omitempty"`
}

func (p *Product) validate() error {
	for k, v := range p.DTAPRenderings {
		if err := v.validate(); err != nil {
			return fmt.Errorf("product '%s', dtap_rendering: '%s': %w", p.ID, k, err)
		}
	}
	for k, v := range p.UserGroupRenderings {
		if err := v.validate(); err != nil {
			return fmt.Errorf("product '%s', user_group_rendering: '%s': %w", p.ID, k, err)
		}
	}
	return nil
}
