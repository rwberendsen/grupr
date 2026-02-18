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

// caching objects in Snowflake locally
type accountCache struct {
	mu       sync.RWMutex // guards dbs, dbExists, and version
	version  int
	dbs      map[semantics.Ident]*dbCache // nil: never requested; empty: none found
	dbExists map[semantics.Ident]bool
}

func (c *accountCache) match(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, om semantics.ObjMatcher, o *matchedAccountObjs) error {
	// will modify both c and o
	err := c.matchDBs(ctx, synCnf, cnf, conn, om, o)
	if err != nil {
		return err
	}
	for db, dbObjs := range o.getDBs() {
		err := c.matchSchemas(ctx, conn, db, om, dbObjs)
		if err != nil {
			return err
		}
		for schema, schemaObjs := range dbObjs.getSchemas() {
			err = c.matchObjects(ctx, conn, db, schema, om, schemaObjs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *accountCache) matchDBs(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, om semantics.ObjMatcher, o *matchedAccountObjs) error {
	c.mu.Lock() // block till all another writer or any active readers are done, get a write lock, now you are the only one modifying the tree
	defer c.mu.Unlock()
	if o.version == c.version {
		// cache entry is stale
		err := c.refreshDBs(ctx, synCnf, cnf, conn)
		if err != nil {
			return err
		}
	}
	o.version = c.version
	for k, _ := range o.getDBs() {
		if !c.hasDB(k) {
			o.dropDB(k)
		}
	}
	for k := range c.getDBs() {
		if !om.DisjointFromDB(k) {
			o.addDB(k)
		}
	}
	return nil
}

func (c *accountCache) matchSchemas(ctx context.Context, conn *sql.DB, db semantics.Ident, om semantics.ObjMatcher, o *matchedDBObjs) error {
	c.mu.RLock() // Block till a (requesting) writer (obtains and) releases the lock, if any, get a read lock, now you can read this node,
	// concurrently with other readers
	defer c.mu.RUnlock()
	if !c.hasDB(db) {
		// Another thread may have modified c, refreshing db's, and deleted this db.
		return ErrObjectNotExistOrAuthorized
	}
	// It could still be that o.Version < c.version
	// I'm fine with that, as long as the db I'm interested is still there in the current version
	//	This works, because the kind of db is in the db key; if it weren't for all I know everything is fine, but the db all of a sudden
	//	is not a standard db anymore; it is an imported db. Which I might want to treat differently.
	c.dbs[db].mu.Lock() // get a write lock on this database
	defer c.dbs[db].mu.Unlock()
	if o.version == c.dbs[db].version {
		// cache entry is stale
		err := c.dbs[db].refreshSchemas(ctx, conn, db)
		if err != nil {
			return err
		}
	}
	o.version = c.dbs[db].version
	for k, _ := range o.getSchemas() {
		if !c.dbs[db].hasSchema(k) {
			o.dropSchema(k)
		}
	}
	for k := range c.dbs[db].getSchemas() {
		if !om.DisjointFromSchema(db, k) {
			o.addSchema(k)
		}
	}
	return nil
}

func (c *accountCache) matchObjects(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident, om semantics.ObjMatcher, o *matchedSchemaObjs) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.hasDB(db) {
		return ErrObjectNotExistOrAuthorized
	}
	c.dbs[db].mu.RLock()
	defer c.dbs[db].mu.RUnlock()
	if !c.dbs[db].hasSchema(schema) {
		return ErrObjectNotExistOrAuthorized
	}
	c.dbs[db].schemas[schema].mu.Lock() // get a (write) lock on this schema
	defer c.dbs[db].schemas[schema].mu.Unlock()
	if o.version == c.dbs[db].schemas[schema].version {
		// cache entry is stale
		err := c.dbs[db].schemas[schema].refreshObjects(ctx, conn, db, schema)
		if err != nil {
			return err
		}
	}
	o.version = c.dbs[db].schemas[schema].version
	// Next, we overwrite whatever objects o may have had; but note that we would have set it to nil to save memory; see schema_objs.go
	o.objects = map[semantics.Ident]ObjAttr{}
	for k, v := range c.dbs[db].schemas[schema].objects {
		if !om.DisjointFromObject(db, schema, k) {
			o.objects[k] = v
		}
	}
	return nil
}

func (c *accountCache) refreshDBs(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	// Do not directly call this function, meant to be called only via match and friends,
	// which would have required appropriate write locks to mutexes
	dbs, err := queryDBs(ctx, conn)
	if err != nil {
		return err
	}
	for k, _ := range c.getDBs() {
		if _, ok := dbs[k]; !ok {
			c.dropDB(k)
		}
	}
	for k := range dbs {
		if !c.hasDB(k) {
			if err := c.addDB(ctx, synCnf, cnf, conn, k); err != nil {
				return err
			}
		}
	}
	c.version += 1
	return nil
}

func (c *accountCache) addDB(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, k semantics.Ident) error {
	if c.dbs == nil {
		c.dbs = map[semantics.Ident]*dbCache{}
		c.dbExists = map[semantics.Ident]bool{}
	}
	if _, ok := c.dbs[k]; !ok {
		c.dbs[k] = &dbCache{}
	}
	// After a DB has been dropped and recreated, DB roles may have been dropped
	if err := c.dbs[k].refreshDBRoles(ctx, synCnf, cnf, conn, k); err != nil {
		return err
	}
	c.dbExists[k] = true
	return nil
}

func (c *accountCache) dropDB(k semantics.Ident) {
	if _, ok := c.dbs[k]; !ok {
		panic(fmt.Sprintf("database not found: '%s'", k))
	}
	c.dbExists[k] = false
}

func (c *accountCache) hasDB(k semantics.Ident) bool {
	return c.dbExists != nil && c.dbExists[k]
}

func (c *accountCache) getDBs() iter.Seq2[semantics.Ident, *dbCache] {
	return func(yield func(semantics.Ident, *dbCache) bool) {
		for k, v := range c.dbs {
			if c.hasDB(k) {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

func queryDBs(ctx context.Context, conn *sql.DB) (map[semantics.Ident]struct{}, error) {
	dbs := map[semantics.Ident]struct{}{}
	start := time.Now()
	log.Printf("Querying Snowflake for database names...\n")
	// TODO: Develop models (if any) for working with IMPORTED DATABASE, and APPLICATION DATABASE
	// TODO: When there are more than 10K results, paginate
	rows, err := conn.QueryContext(ctx, `SHOW TERSE DATABASES IN ACCOUNT ->> SELECT "name" FROM $1 WHERE "kind" = 'STANDARD'`)
	if err != nil {
		return nil, fmt.Errorf("queryDBs error: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var db semantics.Ident
		if err = rows.Scan(&db); err != nil {
			return nil, fmt.Errorf("queryDBs: error scanning row: %w", err)
		}
		if _, ok := dbs[db]; ok {
			return nil, fmt.Errorf("duplicate db: %v", db)
		}
		dbs[db] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("queryDBs: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for database names took %v\n", t.Sub(start))
	return dbs, nil
}
