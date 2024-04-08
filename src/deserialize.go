package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

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

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatalf("command line arguments not equal to one")
	}
	fmt.Printf("args: %v\n", flag.Args())
	data, err := ioutil.ReadFile(flag.Arg(0))
	if err != nil {
		log.Fatalf("main: %v", err)
	}
	grups := Grups{}
	err = yaml.Unmarshal([]byte(data), &grups)
	if err != nil {
		log.Fatalf("main: %v", err)
	}
	fmt.Printf("--- grups:\n%v\n\n", grups)
}
