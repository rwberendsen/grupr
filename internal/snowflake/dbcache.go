package snowflake

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"
)

type dbCache struct {
	dbName      string
	dbKind		string
	mu 		sync.Mutex // guards schemas and version
	schemas     map[string]*schemaCache // nil: never requested; empty: none found
	version int
}

func newDBCache(dbName string, dbKind string) *dbCache {
	return &dbCache{dbName: dbName, dbKind: dbKind}
}

func (c *dbCache) getSchemas(dbVersion int) (map[string]*schemaCache, int, error) {
	// Thread-safe method to get schemas in a database
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == nil { c.schemas = map[string]*schemaCache{} }
	// This check is done cause another thread may have refreshed already.
	// 
	// Note that dbVersion can be > c.version if this DB was dropped in
	// Snowflake and later recreated between when this thread last called
	// and now; in this case it is correct to query again.
	if dbVersion < c.version {
		return c.schemas, c.version, nil
	}
	err := c.addSchemas()
	if err != nil { return c.schemas, c.version, err }
	c.version += 1
	return c.schemas, c.version, nil
}

func (c *dbCache) addSchemas() error {
	// Do not directly call this function, meant to be called only from dbCache.getSchemas
	schemaNames, err := querySchemas(c.dbName)
	if err != nil { return err }
	for schemaName, _ := range c.schemas {
		if _, ok := schemaNames[dbName]; !ok {
			delete(c.dbs, dbName)
		}
	}
	for schemaName, _ := range schemaNames {
		if _, ok := c.schemas[schemaName]; !ok {
			c.schemas[schemaName] = newSchemaCache(c.dbBame, schemaName)
		}
	}
	return nil
}

func querySchemas(dbName string) (map[string]bool, error) {
	schemas := map[string]bool{}
	start := time.Now()
	log.Printf("Querying Snowflake for schema  names in DB: %s ...\n", dbName)
	rows, err := getDB().Query(`SHOW TERSE SCHEMAS IN DATABASE IDENTIFIER(?) ->> SELECT "name" FROM S1`, dbName)
	if err != nil {
		return nil, fmt.Errorf("querySchemas error: %w", err)
	}
	for rows.Next() {
		var schemaName string
		if err = rows.Scan(&schemaName); err != nil {
			return nil, fmt.Errorf("queryDBs: error scanning row: %w", err)
		}
		if _, ok := schemas[schemaName]; ok { return nil, fmt.Errorf("duplicate schema name: %s", schemaName) }
		schemas[schemaName] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("querySchemas: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for schema names in DB: %s took %v\n", dbName, t.Sub(start))
	return schemas, nil
}
