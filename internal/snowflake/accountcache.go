package snowflake

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"
)

// caching objects in Snowflake locally
type accountCache struct {
	// TODO: think about whether it makes sense to cache also privileges granted to (database) roles
	mu	sync.RWMutex // guards dbs and version
	dbs     map[dbKey]*dbCache // nil: never requested; empty: none found
	version int
}

func newAccountCache() *accountCache {
	return &accountCache{}
}

func escapeIdentifier(s string) string {
	return strings.ReplaceAll(s, "\"", "\"\"")
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func (c *accountCache) hasDB(db dbKey) bool {
	if v, ok := c.dbs[db]; ok {
		return !v.dropped
	}
	return false
}

func (c *accountCache) match(ctx context.Context, conn *sql.DB, e semantics.ObjExpr, o *AccountObjs) error {
	// will modify both c and o
	retry_requested, err := c.matchDBs(ctx, e[semantics.Database], o)
	if err != nil { return err }
	for db, dbObjs := range o.DBs {
		retryRequested, err := c.matchSchemas(ctx, conn, db, e, dbObjs)
		if err != nil { return err }
		if retryRequested {
			return match(ctx, conn, e, o) // start over, note that o remains modified
		}
		for schema, schemaObjs := range dbObjs.Schemas {
			retryRequested, err = c.matchObjects(ctx, conn, db, schema, e, schemaObjs)
			if err != nil { return err }
			if retryRequested {
				return c.match(ctx, conn, e, o) // start over, note that o remains modified
			}
		}
	}
}

func (c *accountCache) matchDBs(ctx context.Context, conn *sql.DB, ep semantics.ObjExprPart, o *AccountObjs) error {
	c.mu.Lock() // block till all another writer or any active readers are done, get a write lock, now you are the only one modifying the tree
	defer c.mu.Unlock()
	if o.Version == c.version {
		// cache entry is stale
		err := c.refreshDBs(ctx, conn)
		if err != nil { return err }
	}
	o.Version = c.version
	for k := range o.DBs {
		if !c.hasDB(k) {
			delete(o.DBs, k)
		}
	}
	for k := range c.dbs {
		if matchPart(ep, k.name) {
			o.addDB(k)
		}
	}
	return nil
}

func (c *accountCache) matchSchemas(ctx context.Context, conn *sql.DB, db dbKey, ep semantics.ObjExprPart, o *DBObjs) (bool, error) {
	c.mu.RLock() // Block till a (requesting) writer (obtains and) releases the lock, if any, get a read lock, now you can read this node, 
		     // concurrently with other readers
	defer c.mu.RUnlock()
	if _, ok := c.dbs[db]; !ok {
		// Another thread may have modified c, refreshing db's, and deleted this db.
		return true, nil
	}
	// It could still be that o.Version < c.version
	// I'm fine with that, as long as the db I'm interested is still there in the current version
	//	This works, because the kind of db is in the db key; if it weren't for all I know everything is fine, but the db all of a sudden
	//	is not a standard db anymore; it is an imported db. Which I might want to treat differently.
	c.dbs[db].mu.Lock() // get a write lock on this database
	defer c.dbs[db].mu.Unlock()
	if o.Version == c.dbs[db].version {
		// cache entry is stale
		err := c.dbs[db].refreshSchemas(ctx, conn, db.name)
		if err != nil { return false, err } // TODO: if err is obj not exist then request retry
	}
	o.Version = c.dbs[db].version
	for schema := range o.Schemas {
		if !c.dbs[db].hasSchema(schema) {
			delete(o.Schemas, schema)
		}
	}
	for schema := range c.dbs[db].schemas {
		if matchPart(ep, schema) {
			o.addSchema(schema)
		}
	}
	return false, nil
}

func (c *accountCache) matchObjects(ctx context.Context, conn *sql.DB, db dbKey, schema string, ep semantics.ObjExprPart, o *SchemaObjs) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.dbs[db]; !ok { return true, nil }
	c.dbs[db].mu.RLock()
	defer c.dbs[db].mu.RUnlock()
	if _, ok := c.dbs[db].schemas[schema]; !ok { return true, nil }
	c.dbs[db].schemas[schema].mu.Lock() // get a write lock on this schema
	defer c.dbs[db].schemas[schema].mu.Unlock()
	if o.Version == c.dbs[db].schemas[schema].version {
		// cache entry is stale
		err := c.refreshObjects(ctx, conn, db.name, schema)
		if err != nil { return false, err } // TODO: if err is obj not exist then request retry
	}
	o.Version = c.dbs[db].schemas[schema].version
	for objKey := range o.Objects {
		if _, ok := c.dbs[db].schemas[schema].objects[objKey]; !ok {
			delete(o.Objects, objKey)
		}
	}
	for objKey := range c.dbs[db].schemas[schema].objects {
		if matchPart(ep, objKey.name) {
			o.addObject(objKey)
		}
	}
	return false, nil
}

func (c *accountCache) refreshDBs(ctx context.Context, conn *sql.DB) error {
	// Do not directly call this function, meant to be called only via match and friends,
	// which would have required appropriate write locks to mutexes
	dbs, err := queryDBs(ctx, conn)
	if err != nil { return err }
	for k, v := range c.dbs {
		if _, ok := dbs[k]; !ok {
			v.drop()
		}
	}
	for k := range dbs {
		c.addDB(k)
	}
	return nil
}

func (c *accountCache) addDB(k dbKey) {
	if _, ok := c.dbs[k]; !ok {
		if c.dbs == nil {
			c.dbs = map[dbKey]*dbCache{}
		}
		c.dbs[k] = &dbCache{}
		return
	}
	c.dbs[k].createIfDropped()
}

func queryDBs(ctx context.Context, conn *sql.DB) (map[dbKey]true, error) {
	dbs := map[dbKey]string{}
	start := time.Now()
	log.Printf("Querying Snowflake for database names...\n")
	// TODO: consider how much work it would be to support APPLICATION DATABASE
	rows, err := conn.QueryContext(ctx, `SHOW TERSE DATABASES IN ACCOUNT ->> SELECT "name", "kind" FROM S1 WHERE "kind" IN ('STANDARD', 'IMPORTED DATABASE')`)
	if err != nil {
		return nil, fmt.Errorf("queryDBs error: %w", err)
	}
	for rows.Next() {
		var dbName string
		var dbKind string
		if err = rows.Scan(&dbName, &dbKind); err != nil {
			return nil, fmt.Errorf("queryDBs: error scanning row: %w", err)
		}
		db := dbKey{dbName, dbKind}
		if _, ok := dbs[db]; ok { return nil, fmt.Errorf("duplicate db: %v", db) }
		dbs[db] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("queryDBs: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for database names took %v\n", t.Sub(start))
	return dbs, nil
}
