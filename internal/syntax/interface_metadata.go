package syntax

type InterfaceMetadata struct {
	Classification string   `yaml:",omitempty"`
	UserGroups     []string `yaml:"user_groups,flow,omitempty"`
	Objects        []string `yaml:",omitempty"`
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
	MaskColumns    []string `yaml:"mask_columns,omitempty"`
	HashColumns    []string `yaml:"hash_columns,omitempty"`
	ForProduct     *string  `yaml:"for_product",omitempty"`
}
