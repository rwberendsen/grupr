package snowflake

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"
)

type schemaCache struct {
	dbName     string
	dbKind	   string
	schemaName string
	mu		sync.Mutex // guards objects and version
	objects		map[string]int // nil: never requested; empty: none present; value: 0: table, 1: view
	version int
}

func newSchemaCache(dbName string, dbKind string, schemaName string) *schemaCache {
	return &schemaCache{dbName: dbName, dbKind: dbKind, schemaName: schemaName}
}

func (c *accountCache) getDBs(accountVersion int) (map[string]*dbCache, int, error) {
	// WIP: adapt to schemaCache method
	// Thread-safe method to get databases in an account
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dbs == nil { c.dbs = map[string]*dbCache{} }
	// below check is done because another thread may have already refreshed,
	// in which case we don't need to go and fetch databases again
	if accountVersion < c.version {
		return c.dbs, c.version, nil
	}
	err := c.addDBs()
	if err != nil { return c.dbs, c.version, err }
	c.version += 1
	return c.dbs, c.version, nil
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
