package syntax

type Product struct {
	Id	       string
	Classification string
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn string		    `yaml:"user_group_column,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	Consumes       []InterfaceId   	    `yaml:",omitempty"`
	MaskColumns	[]string	    `yaml:"mask_columns"`
}
