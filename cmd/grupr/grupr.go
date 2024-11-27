package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/snowflake"
	"github.com/rwberendsen/grupr/internal/util"
)

func main() {
	oldFlag := flag.String("o", "", "old YAML, if any")
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatalf("need one argument with path to YAML")
	}
	fmt.Printf("args: %v\n", flag.Args())

	newGrupin, err := util.GetGrupinFromPath(flag.Arg(0))
	if err != nil {
		log.Fatalf("get new grupin: %w", err)
	}
	fmt.Printf("--- newGrupin:\n%v\n\n", newGrupin)

	if *oldFlag != "" {
		oldGrupin, err := util.GetGrupinFromPath(*oldFlag)
		if err != nil {
			log.Fatalf("get old grupin: %w", err)
		}
		fmt.Printf("--- oldGrupin:\n%v\n\n", oldGrupin)

		grupinDiff := semantics.NewGrupinDiff(oldGrupin, newGrupin)
		fmt.Printf("--- grupinDiff:\n%v\n\n", grupinDiff)

		// now we can work with the diff: created, deleted, updated.
		// e.g., first created.
		// we can get all tables / views from snowflake, and start
		// expanding the object (exclude) expressions to sets of matching tables.
		snowflakeGrupinDiff := snowflake.NewGrupinDiff(grupinDiff)
		fmt.Printf("%v", snowflakeGrupinDiff)
	}

	snowflakeNewGrupin := snowflake.NewGrupin(newGrupin)
	fmt.Printf("--- snowflakeNewGrupin:\n%v\n\n", snowflakeNewGrupin)

	basicStats := snowflake.NewBasicStats(newGrupin, snowflakeNewGrupin)
	err = snowflake.PersistInSnowflake(basicStats)

	if err != nil {
		log.Fatalf("persisting stats: %v", err)
	}
}
