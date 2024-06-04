package syntax

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type Grups struct {
	Products map[string]Product
}

type Product struct {
	DTAPs          []string             `yaml:",flow,omitempty"`
	UserGroups     []string             `yaml:",flow,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	Interfaces     map[string]Interface `yaml:",omitempty"`
	Consumes       []ProductInterface   `yaml:",omitempty"`
}

type ProductInterface struct {
	Product   string `yaml:"product"`
	Interface string `yaml:"interface"`
}

type Interface struct {
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
}

func NewGrups(data []byte) (Grups, error) {
	grups := Grups{}
	err := yaml.Unmarshal(data, &grups)
	if err != nil {
		return grups, fmt.Errorf("unmarshalling groups: %s", err)
	}
	return grups, nil
}

func (grups *Grups) String() string {
	data, err := yaml.Marshal(grups)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
