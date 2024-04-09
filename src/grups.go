package main

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type Grups struct {
	Products []Product `yaml:"products"`
}

type Product struct {
	Id             string      `yaml:"id"`
	Envs           []string    `yaml:"envs"`
	Objects        []string    `yaml:"objects"`
	ObjectsExclude []string    `yaml:"objects_exclude"`
	Interfaces     []Interface `yaml:"interfaces"`
}

type Interface struct {
	Id             string   `yaml:"id"`
	Objects        []string `yaml:"objects"`
	ObjectsExclude []string `yaml:"objects_exclude"`
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
