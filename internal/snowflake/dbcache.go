package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"strings"
	"sync"
	"time"
)

type dbCache struct {
	mu 		sync.Mutex // guards schemas, schemaExists, and version
	version int
	schemas     map[string]*schemaCache // nil: never requested; empty: none found
	schemaExists map[string]bool
}

func (c *dbCache) addSchema(k string) {
	if c.schemas == nil {
		c.schemas = map[string]*schemaCache{}
		c.schemaExists = map[string]bool{}
	}
	if _, ok := c.schemas[k]; !ok {
		c.schemas[k] = &schemaCache{}
	}
	c.schemaExists[k] = true
}

func (c *dbCache) dropSchema(k string) {
	if _, ok := c.schemas[k]; !ok {
		panic(fmt.Sprintf("Schema not found: '%s'", k))
	}
	c.schemaExists[k] = false
}

func (c *dbCache) hasSchema(k string) {
	return c.schemaExists != nil && c.schemaExists[k];
}

func (c *dbCache) getSchemas() iter.Seq2[string, *schemaCache] {
	return func(yield func(string, *schemaCache) bool) {
		for k, v := range c.schemas {
			if c.schemaExists(k) {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

func (c *dbCache) refreshSchemas(ctx context.Context, conn *sql.DB, dbName string) error {
	// Do not directly call this function, meant to be called only via match and friends,
	// which would have required appropriate write locks to mutexes
	schemas, err := querySchemas(ctx, conn, dbName)
	if err != nil { return err }
	c.version += 1
	for k, _ := range c.schemas {
		if !c.hasSchema(k) { continue }
		if _, ok := schemas[k]; !ok {
			c.dropSchema(k)
		}
	}
	for k := range schemas {
		c.addSchema(k)
	}
	return nil
}

func querySchemas(ctx context.Context, conn *sql.DB, dbName string) (map[string]bool, error) {
	schemas := map[string]bool{}
	start := time.Now()
	log.Printf("Querying Snowflake for schema  names in DB: %s ...\n", dbName)
	// TODO: when there are more than 10K results, paginate
	rows, err := conn.QueryContext(ctx, `SHOW TERSE SCHEMAS IN DATABASE IDENTIFIER(?) ->> SELECT "name" FROM S1`, dbName)
	if err != nil {
		if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
			return nil, ErrObjectNotExistOrAuthorized
		}
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
