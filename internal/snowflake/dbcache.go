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
	dropped 		bool
	mu 		sync.Mutex // guards schemas and version
	schemas     map[string]*schemaCache // nil: never requested; empty: none found
	version int
}

func (c *dbCache) drop() {
	// no need to obtain write lock; only called from accountCache.refreshDBs(),
	// which is called from accountCache.matchDBs, which acquired a write lock
	// on the account level 
	c.dropped = true
	version += 1
	for schema, sc := range c.schemas {
		sc.drop()
	}
}

func (c *dbCache) createIfDropped() {
	if c.dropped {
		c.dropped = false
		version += 1
	}	
}

func (c *dbCache) addSchema(k string) {
	if _, ok := c.schemas[k]; !ok {
		if c.schemas == nil {
			c.schemas = map[string]*schemaCache{}
		}
		c.schemas[k] = &schemaCache{}
		return
	}
	c.schemas[k].createIfDropped()
}

func (c *dbCache) refreshSchemas(dbName string) error {
	// Do not directly call this function, meant to be called only via match and friends,
	// which would have required appropriate write locks to mutexes
	schemas, err := querySchemas(dbName)
	if err != nil { return err }
	for k, v := range c.schemas {
		if _, ok := schemas[k]; !ok {
			v.drop()
		}
	}
	for k := range schemas {
		c.addSchema(k)
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
			return nil, fmt.Errorf("querySchemas: error scanning row: %w", err)
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
