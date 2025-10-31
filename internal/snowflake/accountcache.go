package snowflake

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"
)

// caching objects in Snowflake locally
type accountCache struct {
	// TODO: think about whether it makes sense to cache also privileges granted to (database) roles
	mu	sync.Mutex // guards dbs and version
	dbs     map[string]*dbCache
	version int // 0, 1, 2, ..
}

func newAccountCache() *accountCache {
	return &accountCache{}
}

type dbCache struct {
	dbName      string
	mu 		sync.Mutex // guards schemas and version
	schemas     map[string]*schemaCache
	version int
}

type schemaCache struct {
	dbName     string
	schemaName string
	mu		sync.Mutex // guards objects and version
	objects		map[string]int // 0: table, 1: view
	version int
}

func escapeIdentifier(s string) string {
	return strings.ReplaceAll(s, "\"", "\"\"")
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func (c *accountCache) addDBs() {
	c.dbs = map[string]*dbCache{}
	c.addDBs_()    // we'd like to capture empty db's as well
	c.addSchemas() // and empty schema's

	// query tables and views in parallel
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 2) // both routines can send once to errc without blocking
	go addTables(ctx, c, errc)
	go addViews(ctx, c, errc)

	// receive twice from errc to wait for both routines
	for i := 0; i < 2; i++ {
		err := <-errc
		if err != nil {
			cancel()              // in case the other routine was still running, it can stop now
			log.Fatalf("%s", err) // TODO: behave and return error instead
		}
	}
}

func (c *accountCache) addDBs_() {
	start := time.Now()
	log.Printf("Querying Snowflake for database names...\n")
	rows, err := getDB().Query(`SELECT database_name FROM snowflake.account_usage.databases`)
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var dbName string
		if err = rows.Scan(&dbName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.addDB(dbName)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for database names took %v\n", t.Sub(start))
}

func (c *accountCache) addDB(dbName string) {
	if _, ok := c.dbNames[dbName]; !ok {
		c.dbs[dbName] = &dbCache{
			dbName:      dbName,
			schemas:     map[string]*schemaCache{},
		}
	}
}

func (c *accountCache) addSchemas() {
	start := time.Now()
	log.Printf("Querying Snowflake for schema names...\n")
	rows, err := getDB().Query(`select catalog_name, schema_name from snowflake.account_usage.schemata where deleted is null`)
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var dbName string
		var schemaName string
		if err = rows.Scan(&dbName, &schemaName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.addSchema(dbName, schemaName)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for schema names took %v\n", t.Sub(start))
}

func (c *accountCache) addSchema(dbName string, schemaName string) {
	c.addDB(dbName)
	if _, ok := c.dbs[dbName].schemas[schemaName]; !ok {
		c.dbs[dbName].schemaNames[schemaName] = true
		c.dbs[dbName].schemas[schemaName] = &schemaCache{
			dbName:     dbName,
			schemaName: schemaName,
			tableNames: map[string]bool{},
			viewNames:  map[string]bool{},
		}
	}
}

func addTables(ctx context.Context, c *accountCache, errc chan<- error) {
	start := time.Now()
	log.Printf("Querying Snowflake for table names...\n")
	invalidEntries := 0
	rows, err := getDB().QueryContext(ctx, `SELECT table_catalog, table_schema, table_name FROM snowflake.account_usage.tables where deleted is null`)
	if err != nil {
		errc <- err
		return
	}
	for rows.Next() {
		var dbName sql.NullString
		var schemaName sql.NullString
		var tableName sql.NullString
		if err = rows.Scan(&dbName, &schemaName, &tableName); err != nil {
			errc <- err
			return
		}
		if !dbName.Valid || !schemaName.Valid || !tableName.Valid {
			invalidEntries++
		}
		c.addTable(dbName.String, schemaName.String, tableName.String)
	}
	if err := rows.Close(); err != nil {
		errc <- err
		return
	}
	if err = rows.Err(); err != nil {
		errc <- err
		return
	}
	if invalidEntries > 0 {
		log.Printf("WARN: there were entries in snowflake.account_usage.tables where table_catalog, table_schema, or table_name were null")
	}
	errc <- nil // caller will block on receiving err
	t := time.Now()
	log.Printf("Querying Snowflake for table names took %v\n", t.Sub(start))
}

func (c *accountCache) addTable(dbName, schemaName, tableName string) {
	if dbc, ok := c.dbs[dbName]; ok {
		if sc, ok := dbc.schemas[schemaName]; ok {
			sc.tableNames[tableName] = true
		}
	}
	// ignore, table must have been created after we queried dbNames and schemaNames
}

func addViews(ctx context.Context, c *accountCache, errc chan<- error) {
	start := time.Now()
	log.Printf("Querying Snowflake for view names...\n")
	rows, err := getDB().QueryContext(ctx, `SELECT table_catalog, table_schema, table_name FROM snowflake.account_usage.views where deleted is null`)
	if err != nil {
		errc <- err
		return
	}
	for rows.Next() {
		var dbName string
		var schemaName string
		var viewName string
		if err = rows.Scan(&dbName, &schemaName, &viewName); err != nil {
			errc <- err
			return
		}
		c.addView(dbName, schemaName, viewName)
	}
	if err := rows.Close(); err != nil {
		errc <- err
		return
	}
	if err = rows.Err(); err != nil {
		errc <- err
		return
	}
	errc <- nil // caller will block on receiving err
	t := time.Now()
	log.Printf("Querying Snowflake for view names took %v\n", t.Sub(start))
}

func (c *accountCache) addView(dbName, schemaName, viewName string) {
	if dbc, ok := c.dbs[dbName]; ok {
		if sc, ok := dbc.schemas[schemaName]; ok {
			sc.viewNames[viewName] = true
		}
	}
	// ignore, view must have been created after we queried dbNames and schemaNames
}

func (c *accountCache) getDBsNewerThan(AccountVersion int) map[string]bool, int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.version == newerThanAccountVersion {
		c.addDBs()
		c.version += 1
	}
	return c.dbNames, c.version
}

func (c *accountCache) getDBs() (map[string]bool, int) {
	return getDBsNewerThan(0)
}

// below methods are wrappers around attributes, they could be used to
// implement lazy loading but, in this case, we have loaded all tables right in
// the beginning when we request a list of databases
func (c *dbCache) getSchemas() map[string]*schemaCache {
	return c.schemas
}

func (c *schemaCache) getObjectsNewerThan(a *accountCache, accountVersion, db, dbVersion, schema, schemaVersion) map[string]bool {
	dbs, accountVersion := a.getDBsNewerThan(accountVersion)
	if _, ok := dbs[db]; !ok { return (map[string]bool{}, 0) }
	schemas, dbVersion := dbs[db].getSchemasNewerThan(db, dbVersion)
	if _, ok := schemas[schema]; !ok { return (map[string]bool{}, 0) }
	c.mu.Lock()
	defer c.mu.Unlock
	c.getObjectsHelper()
}

func (c *schemaCache) getObjects() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getObjectsHelper()
	// here, or in helper
	start := time.Now()
	log.Printf("Querying Snowflake for view names...\n")
	rows, err := getDB().QueryContext(ctx, `SHOW TERSE OBJECTS IN SCHEMA database.schema`)
	// prepare for error where db or schema no longer exists, can be normal, just return empty list
	if err != nil {
		errc <- err
		return
	}
	for rows.Next() {
		var dbName string
		var schemaName string
		var viewName string
		if err = rows.Scan(&dbName, &schemaName, &viewName); err != nil {
			errc <- err
			return
		}
		c.addView(dbName, schemaName, viewName)
	}
	if err := rows.Close(); err != nil {
		errc <- err
		return
	}
	if err = rows.Err(); err != nil {
		errc <- err
		return
	}
	errc <- nil // caller will block on receiving err
	t := time.Now()
	log.Printf("Querying Snowflake for view names took %v\n", t.Sub(start))
	// TODO: also delete existing objects from cache that are no longer there.
	// TODO: and leave alone entries that are already there.
}

func (c *schemaCache) getViewNames() map[string]bool {
	return c.viewNames
}
