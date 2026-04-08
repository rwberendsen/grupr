package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/snowflake"
)

func main() {
	// oldFlag := flag.String("o", "", "old YAML, if any") // TODO: grupinDiff needs work
	flag.Parse()
	if len(flag.Args()) < 1 || len(flag.Args()) > 2 {
		log.Fatalf("usage: grupr path_to_yaml [path_to_snowflake_yaml]")
	}
	yamlPath := flag.Arg(0)
	var snowflakeYamlPath string
	if len(flag.Args()) == 2 {
		snowflakeYamlPath = flag.Arg(1)	
	}

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
	semCnf, err := semantics.GetConfig()
	if err != nil {
		log.Fatalf("get semantics config: %v", err)
	}
	newGrupin, err := semantics.NewGrupinFromPath(semCnf, yamlPath)
	if err != nil {
		log.Fatalf("get new grupin: %v", err)
	}
	log.Println("Deserialized YAML")

	/* TODO: consider implementing GrupinDiff
	if *oldFlag != "" {
		oldGrupin, err := util.GetGrupinFromPath(*oldFlag)
		if err != nil {
			log.Fatalf("get old grupin: %v", err)
		}

		grupinDiff := semantics.NewGrupinDiff(oldGrupin, newGrupin)

		// now we can work with the diff: created, deleted, updated.
		// e.g., first created.
		// we can get all tables / views from snowflake, and start
		// expanding the object (exclude) expressions to sets of matching tables.
		snowflake.NewGrupinDiff(grupinDiff)
	}
	*/

	// Set up catching signals and context before we do network requests
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-sigs   // block until we receive Signal
		cancel() // cancel context we will use to spawn threads, e.g., that hit our backend, e.g., Snowflake
	}()

	// Get DB connection; calling this only once and passing it around as necessary
	snowCnf, err := snowflake.GetConfig(semCnf)
	if err != nil {
		log.Fatalf("get snowflake config: %v", err)
	}

	conn, err := snowflake.GetDB(ctx, snowCnf)
	if err != nil {
		log.Fatalf("error creating db connection: %v", err)
	}
	log.Println("Connected to the database")

	// Create Snowflake Grupin object, which will hold, later, all relevant account objects per data product
	// Still, this call already initializes the account cache, which will already have all databases that exist,
	// and the database roles that grupr is managing; we might move this initialisation to the ManageAccess call,
	// in which case we would not need to pass in `conn` below at all
	snowflakeNewGrupin, err := snowflake.NewGrupin(ctx, semCnf, snowCnf, conn, newGrupin, snowflakeYamlPath)
	if err != nil {
		log.Fatalf("error NewGrupin: %v", err)
	}
	log.Println("Created snowflake.Grupin object")

	// Use it now to manage access; this will also query Snowflake for which objects exist
	if err := snowflakeNewGrupin.ManageAccess(ctx, semCnf, snowCnf, conn); err != nil {
		log.Fatalf("ManageAccess: %v", err)
	}
	log.Println("Managed access")

	// And, after managing access, which may have resulted in numerous refreshes of which objects exist,
	// let's store the latest object counts
	if err := snowflake.StoreObjCountsRows(ctx, snowCnf, conn, snowflakeNewGrupin.GetObjCountsRows()); err != nil {
		log.Fatalf("StoreObjectCounts: %v", err)
	}

	// TODO: also think about how to guard against an error scenario in which someone triggers an old grupr run in CI/CD, e.g., we could store a UUID, or even a git hash
	// in the Grupr schema of the currently running run; the last thing Grupr would always try before crashing is to wipe that one; but, it'd mean from time to time ops may have
	// to come in and delete that one; but imagine the bewilderment if two grupr processes are concurrently trying to make two different yamls the reality...
}
