package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"text/template"
)

func getEnv(key string) (string, error) {
	val, ok := os.LookupEnv(key)
	if !ok {
		return "", fmt.Errorf("environment variable not found: %s", key)
	}
	return val, nil
}

func getGrupsFromPath(path string) (*Grups, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %s", err)
	}
	tmpl, err := template.New("grups").Funcs(template.FuncMap{"getEnv": getEnv}).Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("parsing template: %s", err)
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, nil); err != nil {
		return nil, fmt.Errorf("rendering template: %s", err)
	}
	grups, err := getGrups(rendered.Bytes())
	if err != nil {
		return nil, fmt.Errorf("getting grups: %s", err)
	}
	if err := grups.validate(); err != nil {
		return nil, fmt.Errorf("validating grups: %s", err)
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
