package syntax

import (
	"fmt"
	"slices"
)

type InterfaceMetadata struct {
	Classification     string    `yaml:",omitempty"`
	UserGroupMapping   string    `yaml:"user_group_mapping,omitempty"`
	UserGroups         []string  `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn    string    `yaml:"user_group_column,omitempty"`
	Objects            []string  `yaml:",omitempty"`
	ObjectsExclude     []string  `yaml:"objects_exclude,omitempty"`
	MaskColumns        []string  `yaml:"mask_columns,omitempty"`
	HashColumns        []string  `yaml:"hash_columns,omitempty"`
	ExposeDTAPs        []string  `yaml:"expose_dtaps,flow,omitempty"`
	UserGroupRendering Rendering `yaml:"user_group_rendering,omitempty"`
	ForProduct         *string   `yaml:"for_product",omitempty"`
}

func (i InterfaceMetadata) validate() error {
	if i.Classification != "" {
		if err := validateID(i.Classification); err != nil {
			return fmt.Errorf("classification: %w", err)
		}
	}
	if i.UserGroupMapping != "" {
		if err := validateID(i.UserGroupMapping); err != nil {
			return fmt.Errorf("user_group_mapping: %w", err)
		}
	}
	for _, u := range i.UserGroups {
		if err := validateID(u); err != nil {
			return fmt.Errorf("UserGroup %s: %w", u, err)
		}
		if err := hasUniqueStrings(i.UserGroups); err != nil {
			return fmt.Errorf("usergroups: %w", err)
		}
	}
	if i.UserGroupColumn != "" {
		if len(i.UserGroups) == 0 {
			return fmt.Errorf("UserGroupColumn specified but not UserGroups")
		}
	}
	if i.Objects == nil && i.ObjectsExclude != nil {
		return fmt.Errorf("no objects specified, but objects to exclude were specified")
	}
	for _, d := range i.ExposeDTAPs {
		if err := validateID(d); err != nil {
			return fmt.Errorf("ExposeDTAPs: %w", err)
		}
	}
	if err := i.UserGroupRendering.validate(); err != nil {
		return fmt.Errorf("UserGroupRendering: %w", err)
	}
	for k, _ := range i.UserGroupRendering {
		if !slices.Contains(i.UserGroups, k) {
			return fmt.Errorf("user_group_rendering: unknown user group: '%s'", k)
		}
	}
	if i.ForProduct != nil {
		if err := validateID(*i.ForProduct); err != nil {
			return fmt.Errorf("ForProduct: %w", err)
		}
	}
	return nil
}
