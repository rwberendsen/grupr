package snowflake

import (
	"fmt"
	"log"
	"strings"
)

// caching objects in Snowflake locally
func newAccountCache() *accountCache {
	return &accountCache{}
}

type accountCache struct {
	dbs     map[string]*dbCache
	dbNames map[string]bool
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
	rows, err := getDB().Query(`SELECT database_name FROM snowflake.information_schema.databases`)
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	log.Print("queried database successfully")
	for rows.Next() {
		log.Print("processing row")
		var dbName string
		if err = rows.Scan(&dbName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		log.Printf("dbName: %v", dbName)
		c.dbs[dbName] = &dbCache{dbName: dbName}
		c.dbNames[dbName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
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

func (c *dbCache) addSchemas() {
	c.schemas = map[string]*schemaCache{}
	c.schemaNames = map[string]bool{}
	rows, err := getDB().Query(fmt.Sprintf(`SELECT schema_name FROM IDENTIFIER('"%s".information_schema.schemata')`, escapeIdentifier(c.dbName)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var schemaName string
		if err = rows.Scan(&schemaName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.schemas[schemaName] = &schemaCache{dbName: c.dbName, schemaName: schemaName}
		c.schemaNames[schemaName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (c *dbCache) getSchemas() map[string]*schemaCache {
	if c.schemas == nil {
		c.addSchemas()
	}
	return c.schemas
}

func (c *dbCache) getSchemaNames() map[string]bool {
	if c.schemaNames == nil {
		c.addSchemas()
	}
	return c.schemaNames
}

func (c *schemaCache) addTables() {
	c.tableNames = map[string]bool{}
	rows, err := getDB().Query(fmt.Sprintf(`SELECT table_name FROM "%s".information_schema.tables WHERE table_schema = '%s'`,
		escapeIdentifier(c.dbName), escapeString(c.schemaName)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.tableNames[tableName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (c *schemaCache) getTableNames() map[string]bool {
	if c.tableNames == nil {
		c.addTables()
	}
	return c.tableNames
}

func (c *schemaCache) addViews() {
	c.viewNames = map[string]bool{}
	rows, err := getDB().Query(fmt.Sprintf(`SELECT table_name FROM "%s".information_schema.views WHERE table_schema = '%s'`,
		escapeIdentifier(c.dbName), escapeString(c.schemaName)))
	if err != nil {
		log.Fatalf("querying snowflake: %s", err)
	}
	for rows.Next() {
		var viewName string
		if err = rows.Scan(&viewName); err != nil {
			log.Fatalf("error scanning row: %s", err)
		}
		c.tableNames[viewName] = true
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (c *schemaCache) getViewNames() map[string]bool {
	if c.viewNames == nil {
		c.addViews()
	}
	return c.viewNames
}
