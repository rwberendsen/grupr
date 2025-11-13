package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signals"
	"syscall"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/snowflake"
	"github.com/rwberendsen/grupr/internal/util"
)

func main() {
	// oldFlag := flag.String("o", "", "old YAML, if any") // TODO: grupinDiff needs work
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

	/*
	TODO: grupinDiff needs work, focusing on Grupin for now
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
	} */

	// Set up catching signals and context before we do network requests
	sigs := make(chan signal.Signal, 1)
	signals.Notify(sigs, syscall.SIGTERM)
	ctx, cancel := context.WithCancel()
	defer cancel()
	go func() {
		<- sigs // block until we receive Signal
		cancel() // cancel context we will use to spawn threads, e.g., that hit our backend, e.g., Snowflake
	}()


	// Get DB connection; calling this only once and passing it around as necessary
	cnf, err := snowflake.GetConfig()

	db, err := snowflake.GetDB(ctx, cnf)
	if err != nil { log.Fatalf("error creating db connection: %v", err) }

	// Create Snowflake Grupin object, which will hold relevant account objects per data product
	snowflakeNewGrupin, err := snowflake.NewGrupin(ctx, db, newGrupin)
	if err != nil { log.Fatalf("making Snowflake grupin: %v", err) }

	// TODO: think about it, do we first build our snowflake grupin, which until now just contains matched objects and that's it; and then we start looping over that one
	// to work out what database roles to create, and what privileges to grant to each of them? Or, do we already when building the grupin also work out and store what
	// (database) roles and what granted privileges already exist in each of them? (as well as between them, with consume relations, much like in semantics.Grupin?)
	// The latter is more appealing at first thought, more closely following what semantics.Grupin contains as well.
	// And then, when we have that entire decorated Christmas tree, then we loop over that one to compute what statements we would have to execute to make the reality as we found
	// it in Snowflake reflect what we want it to be in our yaml. And at that point, if we then encounter issues such as objects that have been deleted, then we could use
	// cheap methods to refresh a particular product in the Snowflake grupin object. In particular because we are going to batch GRANT statements; the batch will fail even if
	// just one statement fails; we could try to figure out which one(s) failed, omit them, and re-run the batch, but, it might be smarter to recompute the objects we think are
	// there and then retry the whole batch, recomputing the grant statements we want to run. And if we are going to be able to recompute a product, we can also do this already
	// if we detect objects being deleted out from under us while we are creating the Snowflake grupin object. Won't hurt.

	// TODO: also think about how to guard against an error scenario in which someone triggers an old grupr run in CI/CD, e.g., we could store a UUID, or even a git hash
	// in the Grupr schema of the currently running run; the last thing Grupr would always try before crashing is to wipe that one; but, it'd mean from time to time ops may have
	// to come in and delete that one; but imagine the bewilderment if two grupr processes are concurrently trying to make two different yamls the reality...

	basicStats := snowflake.NewBasicStats(newGrupin, snowflakeNewGrupin)
	err = snowflake.PersistInSnowflake(ctx, db, basicStats)

	if err != nil { log.Fatalf("persisting stats: %v", err) }
}
