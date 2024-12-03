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

	// TODO: while deserializing into Grupin, also gunzip, and
	// calculate hash based on gzipped bytes. (using something like, io.TeeReader)

	// at the same time, download the existing gzipped file from S3, if any, and
	// compute it's running hash. Also capture the Etag.

	// Now, if the hash is the same, we can just stop (all good, nothing to change)
	// If the hash is different though, then we should do an S3 upload.
        // Because this script may run on distributed compute, it should be idempotent.
	// We will use a conditional write, and only overwrite if the Etag of the object
	// has not changed since we downloadded the file.

	// Since we can only decide to write until after we've read in the whole file,
        // we'll need to keep the whole file in memory; unless we could write it to a
	// temp key in S3 and then copy them; S3 CopyObject does support condtional write
	// headers; most likely they would be applied on the target object for the copy
	// operation. So, yeah, most likely this would work.
	newGrupin, err := util.GetGrupinFromPath(flag.Arg(0))
	if err != nil {
		log.Fatalf("get new grupin: %v", err)
	}
	fmt.Printf("--- newGrupin:\n%v\n\n", newGrupin)

	if *oldFlag != "" {
		oldGrupin, err := util.GetGrupinFromPath(*oldFlag)
		if err != nil {
			log.Fatalf("get old grupin: %v", err)
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
