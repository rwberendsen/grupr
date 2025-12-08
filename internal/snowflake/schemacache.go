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
	dropped 	bool
	mu		sync.Mutex // guards objects and version
	objects		map[objKey]struct // nil: never requested; empty: none present; value: TABLE or VIEW
	version int
}

func (c *schemaCache) drop() {
	// no need to obtain write lock; only called in cases where
	// a write lock on the database or account level has been acquired already 
	c.dropped = true
	version += 1
	objects = nil
}

func (c *schemaCache) createIfDropped() {
	if c.dropped {
		c.dropped = false
		version += 1
	}	
}

func (c *schemaCache) refreshObjects(ctx context.Context, conn *sql.DB, dbName string, schemaName string) error {
	// Do not directly call this function, meant to be called only from schemaCache.getObjects
	objects, err := queryObjects(ctx, conn, dbName, schemaName)
	if err != nil { return err }
	c.version += 1
	c.objects = objects
	return nil
}

func queryObjects(ctx context.Context, conn *sql.DB, dbName string, schemaName string) (map[objKey]struct, error) {
	objects := map[objKey]struct{}
	start := time.Now()
	log.Printf("Querying Snowflake for object names in schema: %s.%s ...\n", dbName, schemaName)
	rows, err := getDB().Query(`SHOW TERSE OBJECTS IN SCHEMA IDENTIFIER(?) ->> SELECT "name", "kind" FROM S1`, dbName + "." + schemaName)
	if err != nil {
		if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
			return nil, ErrObjectNotExistOrAuthorized
		}
		return nil, fmt.Errorf("queryObjects error: %w", err)
	}
	for rows.Next() {
		var objectName string
		var objectKind string
		if err = rows.Scan(&objectName, &objectKind); err != nil {
			return nil, fmt.Errorf("queryObjectss: error scanning row: %w", err)
		}
		k := objKey{objectName, objectKind}
		if _, ok := objects[k]; ok { return nil, fmt.Errorf("duplicate object: %v", k) }
		object[k] = struct{}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("queryObjects: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for object names in schema: %s.%s took %v\n", dbName, schemaName, t.Sub(start))
	return objects, nil
}
