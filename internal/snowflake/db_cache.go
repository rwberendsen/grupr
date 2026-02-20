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

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type dbCache struct {
	mu           sync.RWMutex // guards schemas, schemaExists, and version
	version      int
	schemas      map[semantics.Ident]*schemaCache // nil: never requested; empty: none found
	schemaExists map[semantics.Ident]bool
	dbRoles      map[DatabaseRole]struct{}
}

func (c *dbCache) addSchema(k semantics.Ident) {
	if c.schemas == nil {
		c.schemas = map[semantics.Ident]*schemaCache{}
		c.schemaExists = map[semantics.Ident]bool{}
	}
	if _, ok := c.schemas[k]; !ok {
		c.schemas[k] = &schemaCache{}
	}
	c.schemaExists[k] = true
}

func (c *dbCache) dropSchema(k semantics.Ident) {
	if _, ok := c.schemas[k]; !ok {
		panic(fmt.Sprintf("Schema not found: '%s'", k))
	}
	c.schemaExists[k] = false
}

func (c *dbCache) hasSchema(k semantics.Ident) bool {
	return c.schemaExists[k]
}

func (c *dbCache) getSchemas() iter.Seq2[semantics.Ident, *schemaCache] {
	return func(yield func(semantics.Ident, *schemaCache) bool) {
		for k, v := range c.schemas {
			if c.schemaExists[k] {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

func (c *dbCache) refreshSchemas(ctx context.Context, conn *sql.DB, db semantics.Ident) error {
	// Do not directly call this function, meant to be called only via match and friends,
	// which would have required appropriate write locks to mutexes
	schemas, err := querySchemas(ctx, conn, db)
	if err != nil {
		return err
	}
	c.version += 1
	for k, _ := range c.getSchemas() {
		if _, ok := schemas[k]; !ok {
			c.dropSchema(k)
		}
	}
	for k := range schemas {
		if !c.hasSchema(k) {
			c.addSchema(k)
		}
	}
	return nil
}

func (c *dbCache) refreshDBRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, db semantics.Ident) error {
	c.dbRoles = map[DatabaseRole]struct{}{} // overwrite if c.dbRoles already had a value
	for r, err := range QueryDatabaseRoles(ctx, synCnf, cnf, conn, db) {
		if err != nil {
			return err
		}
		c.dbRoles[r] = struct{}{}
	}
	return nil
}

func querySchemas(ctx context.Context, conn *sql.DB, db semantics.Ident) (map[semantics.Ident]bool, error) {
	schemas := map[semantics.Ident]bool{}
	start := time.Now()
	log.Printf("Querying Snowflake for schema  names in DB: %s ...\n", db)
	// TODO: when there are more than 10K results, paginate
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW TERSE SCHEMAS IN DATABASE IDENTIFIER('%s') ->> SELECT "name" FROM $1`, db))
	if err != nil {
		if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
			return nil, ErrObjectNotExistOrAuthorized
		}
		return nil, fmt.Errorf("querySchemas error: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schema semantics.Ident
		if err = rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("querySchemas: error scanning row: %w", err)
		}
		if len(schema) == 0 {
			return nil, fmt.Errorf("zero-length schema identifier")
		}
		if _, ok := schemas[schema]; ok {
			return nil, fmt.Errorf("duplicate schema name: %s", schema)
		}
		schemas[schema] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("querySchemas: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for schema names in DB: %s took %v\n", db, t.Sub(start))
	return schemas, nil
}
