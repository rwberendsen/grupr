package syntax

type Interface struct {
	Id
	ProductId		string `yaml:"product_id"`
	ProducingServiceId string `yaml:"producing_service_id,omitempty"`
	Classification string `yaml:",omitempty"` // inherits from product if not specified; can be less strict, but not stricter
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	ExposeDTAPS    []string
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn string		    `yaml:"user_group_column,omitempty"`
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
	MaskColumns    []string `yaml:"mask_columns,omitempty"`
	HashColumns    []string `yaml:"hash_columns,omitempty"`
}
