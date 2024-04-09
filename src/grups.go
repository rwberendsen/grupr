package main

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type Grups struct {
	Products []Product `yaml:"products"`
}

type Product struct {
	Id             string
	DTAPs          []string           `yaml:",flow,omitempty"`
	Objects        []string           `yaml:",omitempty"`
	ObjectsExclude []string           `yaml:"objects_exclude,omitempty"`
	Interfaces     []Interface        `yaml:",omitempty"`
	Consumes       []ProductInterface `yaml:",omitempty"`
}

type ProductInterface struct {
	ProductId   string `yaml:"product_id"`
	InterfaceId string `yaml:"interface_id"`
}

type Interface struct {
	Id             string
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
}

func getGrups(data []byte) (*Grups, error) {
	grups := Grups{}
	err := yaml.Unmarshal(data, &grups)
	if err != nil {
		err = fmt.Errorf("unmarshalling groups: %s", err)
		return nil, err
	}
	return &grups, nil
}

func (grups *Grups) String() string {
	data, err := yaml.Marshal(grups)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
