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

var actionFlag = flag.String("action", "ma", "action to perform")
var productFlag = flag.String("product", "", "product ID to perform action on")
var dtaps stringSet = stringSet{}
var interfaces stringSet = stringSet{}
var dtapRoles stringMap = stringMap{}

func init() {
	flag.Var(interfaces, "interfaces", "perform action on these interfaces only")
	flag.Var(dtaps, "dtaps", "perform action on these dtaps only")
	flag.Var(dtapRoles, "dtapRoles", "grant ownership in each dtap to mapped role")
}

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
	action := *actionFlag
	product := *productFlag

	// Validate internal consistency between supplied flags
	isAction := map[string]bool{
		"archive": true,
		"ma":      true,
		"mae":     true,
		"own":     true,
		"got":     true,
		"purge":   true,
	}
	isProductSpecificAction := map[string]bool{
		"archive": true,
		"mae":     true,
		"own":     true,
		"got":     true,
		"purge":   true,
	}
	isDestructiveAction := map[string]bool{
		"mae":   true,
		"own":   true,
		"got":   true,
		"purge": true,
	}
	if !isAction[action] {
		log.Fatalf("unknown action")
	}
	if (product == "") == isProductSpecificAction[action] {
		log.Fatalf("specify a product if and only if you are doing a product specific action")
	}
	if (product == "") && (len(dtaps) > 0 || len(interfaces) > 0) {
		log.Fatalf("dtaps or interfaces specified, but no product")
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

	// Validate command line flags against semantic grupin, start with action its scope
	var actionScope semantics.ActionScope
	if isProductSpecificAction[action] {
		var err error
		actionScope, err = newGrupin.ValidateAction(product, dtaps, interfaces, isDestructiveAction[action])
		if err != nil {
			log.Fatalf("invalid action: %v", err)
		}
	}
	// Validate for specific actions if action-specific flags are specified correctly
	dtapRoleIdents := map[string]semantics.Ident{}
	if action == "got" {
		if len(dtapRoles) == 0 {
			log.Fatalf("no dtapRoles specified for got action")
		}
		// check that we have a role for each dtap, and check that the roles are proper semantics.Ident values
		for dtap := range actionScope.AllDTAPsProdFirst() {
			if _, ok := dtapRoles[dtap]; !ok {
				log.Fatalf("no role to grant ownership to of objects in dtap '%s'", dtap)
			}
			roleIdent, err := semantics.NewIdent(dtapRoles[dtap].S, dtapRoles[dtap].WasQuoted, semCnf.ValidQuotedExpr, semCnf.ValidUnquotedExpr)
			if err != nil {
				log.Fatalf("dtapRoles, dtap '%s', '%s'", dtap, err)
			}
			dtapRoleIdents[dtap] = roleIdent
		}
		// also check that we did not specify any additional dtaps that we don't have
		for dtap := range dtapRoles {
			if !actionScope.HasDTAP(dtap) {
				log.Fatalf("dtap specified in dtapRoles that is not in action scope")
			}
		}
	} else {
		if len(dtapRoles) > 0 {
			log.Fatalf("dtapRoles specified without got action")
		}
	}

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

	// If it's a dry run, print a clear message that it is a dry run
	if snowCnf.DryRun {
		log.Print(`

================================================================================
||                                                                            ||
||   ########  ########  ##    ##    ########  ##     ##  ##    ##  ####      ||
||   ##     ## ##     ##  ##  ##     ##     ## ##     ##  ###   ##  ####      ||
||   ##     ## ##     ##   ####      ##     ## ##     ##  ####  ##  ####      ||
||   ##     ## ########     ##       ########  ##     ##  ## ## ##   ##       ||
||   ##     ## ##   ##      ##       ##   ##   ##     ##  ##  ####            ||
||   ##     ## ##    ##     ##       ##    ##  ##     ##  ##   ###  ####      ||
||   ########  ##     ##    ##       ##     ##  #######   ##    ##  ####      ||
||                                                                            ||
================================================================================

`)
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

	// Let's check for additional actions for specific products or interfaces
	switch action {
	case "archive":
		// Archive (specified interfaces of) (dtaps of) product ID
		if err := snowflakeNewGrupin.Archive(ctx, snowCnf, conn, actionScope); err != nil {
			log.Fatalf("archive: %v", err)
		}
		log.Printf("Archive action for product '%v' successful", product)
	case "mae":
		// Manage access exclusively, require a product ID in this case
		if err := snowflakeNewGrupin.ManageAccessExclusively(ctx, semCnf, snowCnf, conn, actionScope); err != nil {
			log.Fatalf("mae: %v", err)
		}
		log.Printf("Managed access exclusively for product '%s'", product)
	case "own":
		// Manage access exclusively, require a product ID in this case
		if err := snowflakeNewGrupin.Own(ctx, semCnf, snowCnf, conn, actionScope); err != nil {
			log.Fatalf("mae: %v", err)
		}
		log.Printf("Owned product '%s'", product)
	case "got":
		// Manage access exclusively, require a product ID in this case
		if err := snowflakeNewGrupin.GrantOwnershipTo(ctx, snowCnf, conn, actionScope, dtapRoleIdents); err != nil {
			log.Fatalf("mae: %v", err)
		}
		log.Printf("Granted ownership of objects for product '%s'", product)
	case "purge":
		// Purge (DROP) objects
		if err := snowflakeNewGrupin.Purge(ctx, snowCnf, conn, actionScope); err != nil {
			log.Fatalf("purge: %v", err)
		}
		log.Printf("Purge completed")
	}

	// TODO: also think about how to guard against an error scenario in which someone triggers an old grupr run in CI/CD, e.g., we could store a UUID, or even a git hash
	// in the Grupr schema of the currently running run; the last thing Grupr would always try before crashing is to wipe that one; but, it'd mean from time to time ops may have
	// to come in and delete that one; but imagine the bewilderment if two grupr processes are concurrently trying to make two different yamls the reality...
	// ... perhaps at least, since at this time all we have is a current target yaml, just while we run, grab any kind of lock in Snowflake, which will be released if
	// we lose the connection
}
