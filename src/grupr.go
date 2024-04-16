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
	oldFlag := flag.String("o", "", "old YAML, if any")
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatalf("need one argument with path to YAML")
	}
	fmt.Printf("args: %v\n", flag.Args())

	newGrups, err := getGrupsFromPath(flag.Arg(0))
	if err != nil {
		log.Fatalf("get new grups: %s", err)
	}
	fmt.Printf("--- newGrups:\n%v\n\n", newGrups)

	var oldGrups *Grups
	if *oldFlag != "" {
		oldGrups, err = getGrupsFromPath(*oldFlag)
		if err != nil {
			log.Fatalf("get old groups: %s", err)
		}
		fmt.Printf("--- oldGrups:\n%v\n\n", oldGrups)
	}

	grupsDiff := getGrupsDiff(oldGrups, newGrups)
	fmt.Printf("--- grupsDiff:\n%v\n\n", grupsDiff)
}
