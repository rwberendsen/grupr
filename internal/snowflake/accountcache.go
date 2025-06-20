package snowflake

import (
	"context"
	"log"
	"strings"
)

// caching objects in Snowflake locally
type accountCache struct {
	dbs     map[string]*dbCache
	dbNames map[string]bool
}

func newAccountCache() *accountCache {
	return &accountCache{}
}

type dbCache struct {
	dbName      string
	schemas     map[string]*schemaCache
	schemaNames map[string]bool
}

type schemaCache struct {
	dbName     string
	schemaName string
	// note that if during run time a table is removed, and a view is
	// created with the same name or vice versa then tableNames and
	// viewNames can contain duplicate keys wrt each other
	tableNames map[string]bool
	viewNames  map[string]bool
}

func escapeIdentifier(s string) string {
	return strings.ReplaceAll(s, "\"", "\"\"")
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func (c *accountCache) addDBs() {
	c.dbs = map[string]*dbCache{}
	c.dbNames = map[string]bool{}
	c.addDBs_()     // we'd like to capture empty db's as well
	c.addSchemas()  // and empty schema's

	// query tables and views in parallel	
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 2) // both routines can send once to errc without blocking
	go addTables(ctx, c, errc) 
	go addViews(ctx, c, errc)

	// receive twice from errc to wait for both routines
	for i := 0; i < 2; i++ {
		err := <-errc
		if err != nil {
			cancel() // in case the other routine was still running, it can stop now
			log.Fatalf("%w", err) // TODO: behave and return error instead
		}
	}
}

func (c *accountCache) addDBs_() {
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
}

func (c *accountCache) addDB(dbName string) {
	if _, ok := c.dbNames[dbName]; !ok {
		c.dbNames[dbName] = true
		c.dbs[dbName] = &dbCache{
			dbName: dbName,
			schemas: map[string]*schemaCache{},
			schemaNames: map[string]bool{},
		}
	}
}

func (c *accountCache) addSchemas() {
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
}

func (c *accountCache) addSchema(dbName string, schemaName string) {
	c.addDB(dbName)
	if _, ok := c.dbs[dbName].schemas[schemaName]; !ok {
		c.dbs[dbName].schemaNames[schemaName] = true
		c.dbs[dbName].schemas[schemaName] = &schemaCache{
			dbName: dbName,
			schemaName: schemaName,
			tableNames: map[string]bool{},
			viewNames: map[string]bool{},
		}
	}
}

func addTables(ctx context.Context, c *accountCache, errc chan<- error) {
	rows, err := getDB().QueryContext(ctx, `SELECT table_catalog, table_schema, table_name FROM snowflake.account_usage.tables where deleted is null`)
	if err != nil {
		errc <- err
		return
	}
	for rows.Next() {
		var dbName string
		var schemaName string
		var tableName string
		if err = rows.Scan(&dbName, &schemaName, &tableName); err != nil {
			errc <- err
			return
		}
		c.addTable(dbName, schemaName, tableName)
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
}


func (c *accountCache) addView(dbName, schemaName, viewName string) {
	if dbc, ok := c.dbs[dbName]; ok {
		if sc, ok := dbc.schemas[schemaName]; ok {
			sc.viewNames[viewName] = true
		}
	}
	// ignore, view must have been created after we queried dbNames and schemaNames
}

func (c *accountCache) getDBs() map[string]*dbCache {
	if c.dbs == nil {
		c.addDBs()
	}
	return c.dbs
}

func (c *accountCache) getDBnames() map[string]bool {
	if c.dbNames == nil {
		c.addDBs()
	}
	return c.dbNames
}

// below methods are wrappers around attributes, they could be used to
// implement lazy loading but, in this case, we have loaded all tables right in
// the beginning when we request a list of databases
func (c *dbCache) getSchemas() map[string]*schemaCache {
	return c.schemas
}

func (c *dbCache) getSchemaNames() map[string]bool {
	return c.schemaNames
}

func (c *schemaCache) getTableNames() map[string]bool {
	return c.tableNames
}

func (c *schemaCache) getViewNames() map[string]bool {
	return c.viewNames
}
