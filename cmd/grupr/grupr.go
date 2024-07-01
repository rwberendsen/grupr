package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"text/template"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/snowflake"
	"github.com/rwberendsen/grupr/internal/syntax"
)

func getEnv(key string) (string, error) {
	val, ok := os.LookupEnv(key)
	if !ok {
		return "", fmt.Errorf("environment variable not found: %s", key)
	}
	return val, nil
}

func getGrupsFromPath(path string) (semantics.Grups, error) {
	r := semantics.Grups{}
	data, err := os.ReadFile(path)
	if err != nil {
		return r, fmt.Errorf("reading file: %s", err)
	}
	tmpl, err := template.New("grups").Funcs(template.FuncMap{"getEnv": getEnv}).Parse(string(data))
	if err != nil {
		return r, fmt.Errorf("parsing template: %s", err)
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, nil); err != nil {
		return r, fmt.Errorf("rendering template: %s", err)
	}
	grups, err := syntax.NewGrups(rendered.Bytes())
	if err != nil {
		return r, fmt.Errorf("getting grups: %s", err)
	}
	r, err = semantics.NewGrups(grups)
	if err != nil {
		return r, fmt.Errorf("semantic error: %s", err)
	}
	return r, nil
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

	if *oldFlag != "" {
		oldGrups, err := getGrupsFromPath(*oldFlag)
		if err != nil {
			log.Fatalf("get old groups: %s", err)
		}
		fmt.Printf("--- oldGrups:\n%v\n\n", oldGrups)

		grupsDiff := semantics.NewGrupsDiff(oldGrups, newGrups)
		fmt.Printf("--- grupsDiff:\n%v\n\n", grupsDiff)

		// now we can work with the diff: created, deleted, updated.
		// e.g., first created.
		// we can get all tables / views from snowflake, and start
		// expanding the object (exclude) expressions to sets of matching tables.
		snowflakeGrupsDiff := snowflake.NewGrupsDiff(grupsDiff)
		fmt.Printf("%v", snowflakeGrupsDiff)
	}

	snowflakeNewGrups := snowflake.NewGrups(newGrups)
	fmt.Printf("--- snowflakeNewGrups:\n%v\n\n", snowflakeNewGrups)

	basicStats := snowflake.NewBasicStats(newGrups, snowflakeNewGrups)
	err = snowflake.PersistInSnowflake(basicStats)

	if err != nil {
		log.Fatalf("persisting stats: %v", err)
	}
}
