package syntax

import (
	"fmt"
	"slices"
)

type InterfaceMetadata struct {
	Classification     string    `yaml:",omitempty"`
	UserGroups         []string  `yaml:"user_groups,flow,omitempty"`
	Objects            []string  `yaml:",omitempty"`
	ObjectsExclude     []string  `yaml:"objects_exclude,omitempty"`
	MaskColumns        []string  `yaml:"mask_columns,omitempty"`
	HashColumns        []string  `yaml:"hash_columns,omitempty"`
	ExposeDTAPs        []string  `yaml:"expose_dtaps,flow,omitempty"`
	ForProduct         *string   `yaml:"for_product",omitempty"`
}

func (i InterfaceMetadata) validate() error {
	if i.Classification != "" {
		if err := validateID(i.Classification); err != nil {
			return fmt.Errorf("classification: %w", err)
		}
	}
	for _, u := range i.UserGroups {
		if err := validateIDPart(u); err != nil {
			return fmt.Errorf("UserGroup %s: %w", u, err)
		}
		if err := hasUniqueStrings(i.UserGroups); err != nil {
			return fmt.Errorf("usergroups: %w", err)
		}
	}
	if i.Objects == nil && i.ObjectsExclude != nil {
		return fmt.Errorf("no objects specified, but objects to exclude were specified")
	}
	for _, d := range i.ExposeDTAPs {
		if err := validateIDPart(d); err != nil {
			return fmt.Errorf("ExposeDTAPs: %w", err)
		}
	}
	if i.ForProduct != nil {
		if err := validateIDPart(*i.ForProduct); err != nil {
			return fmt.Errorf("ForProduct: %w", err)
		}
	}
	return nil
}
