package syntax

import (
	"fmt"
	"slices"
)

type Product struct {
	ID                 string            `yaml:"id"`
	DTAPs              *DTAPSpec         `yaml:"dtaps,flow,omitempty"`
	DTAPRendering      Rendering         `yaml:"dtap_rendering,omitempty"`
	UserGroupMappingID   string            `yaml:"user_group_mapping,omitempty"`
	UserGroupColumn    string            `yaml:"user_group_column,omitempty"`
	UserGroupRendering Rendering         `yaml:"user_group_rendering,omitempty"`
	Consumes           []ConsumptionSpec `yaml:"consumption_spec",omitempty"`
	InterfaceMetadata  `yaml:",inline"`
}

func (p *Product) validate(cnf *Config) error {
	if err := validateIDPart(cnf, p.ID); err != nil {
		return err
	}
	if p.DTAPs != nil {
		if err := p.DTAPs.validate(cnf); err != nil {
			return fmt.Errorf("product id: %s, DTAPs: %w", p.ID, err)
		}
		if err := p.DTAPRendering.validate(); err != nil {
			return fmt.Errorf("product '%s': DTAPRendering: %w", p.ID, err)
		}
		for d := range p.DTAPRendering {
			if !p.DTAPs.HasDTAP(d) {
				return &FormattingError{fmt.Sprintf("product '%s': DTAPRendering: unknown dtap '%s'", p.ID, d)}
			}
		}
	} else {
		if p.DTAPRendering != nil {
			return &FormattingError{fmt.Sprintf("product '%s': dtap_rendering specified but no dtaps")}
		}
	}
	if p.UserGroupMappingID != "" {
		if err := validateIDPart(cnf, p.UserGroupMappingID); err != nil {
			return fmt.Errorf("user_group_mapping: %w", err)
		}
	}
	if p.UserGroupColumn != "" {
		if len(p.UserGroups) == 0 {
			return fmt.Errorf("UserGroupColumn specified but not UserGroups")
		}
		if err := validateIDPart(cnf, p.UserGroupColumn); err != nil {
			return fmt.Errorf("user_group_column: %w", err)
		}
	}
	if err := p.UserGroupRendering.validate(); err != nil {
		return fmt.Errorf("UserGroupRendering: %w", err)
	}
	for k, _ := range p.UserGroupRendering {
		if !slices.Contains(p.UserGroups, k) {
			return fmt.Errorf("user_group_rendering: unknown user group: '%s'", k)
		}
	}
	for _, cs := range p.Consumes {
		if err := cs.validate(cnf); err != nil {
			return err
		}
	}
	if err := p.InterfaceMetadata.validate(cnf); err != nil {
		return fmt.Errorf("product %s: %w", p.ID, err)
	}
	return nil
}
