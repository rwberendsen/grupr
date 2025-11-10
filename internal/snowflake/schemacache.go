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
	objects		map[string]string // nil: never requested; empty: none present; value: TABLE or VIEW
	version int
}

func newSchemaCache(dbName string, dbKind string, schemaName string) *schemaCache {
	return &schemaCache{dbName: dbName, dbKind: dbKind, schemaName: schemaName}
}

func (c *schemaCache) getObjects(schemaVersion int) (map[string]*schemaCache, int, error) {
	// Thread-safe method to get databases in an account
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.objects == nil { c.objects = map[string]int{} }
	// This check is done cause another thread may have refreshed already.
	//
	// Note that schemaVersion can be > c.version if this schema was
	// dropped in Snowflake and later recreated between when this thread
	// last called and now; in this case it is correct to query again.
	if schemaVersion < c.version {
		return c.objects, c.version, nil
	}
	err := c.refreshObjects()
	if err != nil { return c.dbs, c.version, err }
	c.version += 1
	return c.dbs, c.version, nil
}

func (c *schemaCache) refreshObjects() error {
	// Do not directly call this function, meant to be called only from schemaCache.getObjects
	objects, err := queryObjects(c.dbName)
	if err != nil { return err }
	c.objects = objects
	return nil
}

func queryObjects(dbName string, schemaName string) (map[string]string, error) {
	objects := map[string]string{}
	start := time.Now()
	log.Printf("Querying Snowflake for object names in schema: %s.%s ...\n", dbName, schemaName)
	rows, err := getDB().Query(`SHOW TERSE OBJECTS IN SCHEMA IDENTIFIER(?) ->> SELECT "name", "kind" FROM S1`, dbName + "." + schemaName)
	if err != nil {
		return nil, fmt.Errorf("queryObjects error: %w", err)
	}
	for rows.Next() {
		var objectName string
		var objectKind string
		if err = rows.Scan(&objectName, &objectKind); err != nil {
			return nil, fmt.Errorf("queryObjectss: error scanning row: %w", err)
		}
		if _, ok := objects[objectName]; ok { return nil, fmt.Errorf("duplicate object name: %s", objectName) }
		object[objectName] = objectKind
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("queryObjects: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for object names in schema: %s.%s took %v\n", dbName, schemaName, t.Sub(start))
	return objects, nil
}
