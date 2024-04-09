package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func getGrupsFromPath(path string) (*Grups, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		err = fmt.Errorf("reading file: %s", err)
		return nil, err
	}
	grups, err := getGrups(data)
	if err != nil {
		err = fmt.Errorf("getting grups: %s", err)
		return nil, err
	}
	return grups, nil
}

func main() {
	flag.Parse()
	prevFlag := flag.String("o", "", "previously submitted YAML, if any")
	if len(flag.Args()) != 1 {
		log.Fatalf("need one argument with path to YAML")
	}
	fmt.Printf("args: %v\n", flag.Args())

	grups, err := getGrupsFromPath(flag.Arg(0))
	if err != nil {
		log.Fatalf("get current grups: %s", err)
	}
	fmt.Printf("--- grups:\n%v\n\n", grups)

	if *prevFlag != "" {
		prevGrups, err := getGrupsFromPath(*prevFlag)
		if err != nil {
			log.Fatalf("get previous groups: %s", err)
		}
		fmt.Printf("--- prevGrups:\n%v\n\n", prevGrups)
	}
}
