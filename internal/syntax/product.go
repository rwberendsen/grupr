package syntax

import (
	"fmt"
	"slices"
)

type Product struct {
	ID                 string            `yaml:"id"`
	DTAPs              *DTAPSpec         `yaml:"dtaps,flow,omitempty"`
	Consumes           []ConsumptionSpec `yaml:"consumes",omitempty"`
	InterfaceMetadata  `yaml:",inline"`
	DTAPRenderings     map[string]Rendering         `yaml:"dtap_renderings,omitempty"`
	UserGroupMappingID string            `yaml:"user_group_mapping,omitempty"`
	UserGroupRenderings map[string]Rendering         `yaml:"user_group_renderings,omitempty"`
	UserGroupColumn    string            `yaml:"user_group_column,omitempty"`
}

func (p *Product) validate(cnf *Config) error {
	if err := ValidateIDPart(cnf, p.ID); err != nil {
		return err
	}
	if err := p.DTAPs.validate(cnf); err != nil {
			return fmt.Errorf("product id: %s, DTAPs: %w", p.ID, err)
	}
	for _, cs := range p.Consumes {
		if err := cs.validate(cnf); err != nil {
			return err
		}
	}
	if err := p.InterfaceMetadata.validate(cnf); err != nil {
		return fmt.Errorf("product %s: %w", p.ID, err)
	}
	for k, v := range p.DTAPRenderings {
		if err := v.validate(); err != nil {
			return fmt.Errorf("product '%s', dtap_rendering: '%s': %w", p.ID, k, err)
		}
	}
	if p.UserGroupMappingID != "" {
		if len(p.UserGroups) == 0 {
			return fmt.Errorf("UserGroupMappingID specified but not UserGroups")
		}
		if err := ValidateIDPart(cnf, p.UserGroupMappingID); err != nil {
			return fmt.Errorf("user_group_mapping: %w", err)
		}
	}
	for k, v := range p.UserGroupRenderings {
		if err := v.validate(); err != nil {
			return fmt.Errorf("product '%s', user_group_rendering: '%s': %w", p.ID, k, err)
		}
		for ug := range v {
			if !slices.Contains(p.UserGroups, k) {
				return fmt.Errorf("product '%s', user_group_rendering: '%s': unknown user group:  %w", p.ID, k, ug)
			}
		}
	}
	if p.UserGroupColumn != "" {
		if len(p.UserGroups) == 0 {
			return fmt.Errorf("UserGroupColumn specified but not UserGroups")
		}
		if err := ValidateIDPart(cnf, p.UserGroupColumn); err != nil {
			return fmt.Errorf("user_group_column: %w", err)
		}
	}
	return nil
}
