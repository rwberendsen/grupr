package syntax

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type ElmntOr struct {
	ProducingService ProducingService `yaml:"producing_service,omitempty"`
	Product Product `yaml:",omitempty"`
	Interface Interface`yaml:"interface,omitempty"`
}

type ProducingService struct {
	Id string
        ObjectsDB string `yaml:"objects_db"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
}

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

type Interface struct {
	Id
	ProductId		string
	ProducingServiceId string
	Classification string `yaml:",omitempty"` // inherits from product if not specified; can be less strict, but not stricter
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	ExposeDTAPS    []string
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn string		    `yaml:"user_group_column,omitempty"`
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
}

type DTAPSpec struct {
	NonProd		[]string		`yaml:"non_prod,flow,omitempty`
	Prod		string
}

type InterfaceId struct {
	ProductId   string `yaml:"product"`
	InterfaceId string `yaml:"interface"`
	ProducingServiceId string `yaml:"producing_service,omitempty"`
}


func NewElmntOr(data []byte) (ElmntOr, error) {
	elmntor := ElmntOr{}
	err := yaml.Unmarshal(data, &elmntor)
	if err != nil {
		return elmntor, fmt.Errorf("unmarshalling elmntor: %s", err)
	}
	return elmntor, nil
}

func (grups Grups) String() string {
	data, err := yaml.Marshal(grups)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
