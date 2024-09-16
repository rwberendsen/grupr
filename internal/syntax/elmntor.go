package syntax

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type ElmntOr struct {
	Product Product `yaml:",omitempty"`
	ProductInterface Interface `yaml:"product_interface,omitempty"`
	ProducingService ProducingService `yaml:"producing_service,omitempty"`
	ProducingServiceInterface Interface `yaml:"producing_service_interface,omitempty"`
}

type Product struct {
	Name           string
	LongName       string			`yaml:"long_name,omitempty`
	Classification string
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	Interfaces     []string `yaml:",omitempty"`
	Consumes       []InterfaceId   `yaml:",omitempty"`
}

type DTAPSpec struct {
	NonProd		[]string		`yaml:"non_prod,flow,omitempty`
	Prod		string
}

type InterfaceId struct {
	Product   string `yaml:"product"`
	Interface string `yaml:"interface"`
	ProducingService string `yaml:"producing_service,omitempty"`
}

type Interface struct {
	Product		string
	Name		string
	LongName	string		`yaml:"long_name,omitempty"`
	Classification string
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
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
