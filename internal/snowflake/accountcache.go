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

func match(e semantics.ObjExpr, c *accountCache, o *AccountObjs) error {
	// will modify both c and o
	retry_requested, err := matchDBs(e[semantics.Database], c, o)
	if err != nil { return err }
	for db := range o.DBs {
		retryRequested, err := matchSchemas(db, e, c, o)
		if err != nil { return err }
		if retryRequested {
			return match(e, c, o) // start over
		}
		for schema := range o.DBs[db].Schemas {
			retryRequested, err = matchObjects(db, schema, e, c, o)
			if err != nil { return err }
			if retryRequested {
				return match(e, c, o) // start over
			}
		}
	}
}

func matchDBs(ep semantics.ObjExprPart, c *accountCache, o *AccountObjs) error {
	c.mu.Lock() // block till all another writer or any active readers are done, get a write lock, now you are the only one modifying the tree
	defer c.mu.Unlock()
	if o.Version == c.version {
		// cache entry is stale
		err := c.refreshDBs()
		if err != nil { return err }
	}
	o.Version = c.version
	for dbKey := range o.DBs {
		if _, ok := c.dbs; !ok {
			delete(o.DBs, dbKey)
		}
	}
	for dbKey := range c.dbs {
		if matchPart(ep, dbKey.name) {
			o.addDB(dbKey)
		}
	}
	return nil
}

func matchSchemas(db dbKey, ep semantics.ObjExprPart, c *accountCache, o *AccountObjs) (bool, error) {
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
	if o.DBs[db].Version == c.dbs[db].version {
		// cache entry is stale
		err := c.refreshSchemas(db)
		if err != nil { return false, err } // TODO: if err is obj not exist then request retry
	}
	o.DBs[db].Version = c.dbs[db].version
	matchedSchemas := matchPart(ep, c.dbs[db].schemas)
	for schema := range o.DBs[db].Schemas {
		if _, ok := c.dbs[db].schemas[schema]; !ok {
			delete(o.DBs[db].Schemas, schema)
		}
	}
	for schema := range c.dbs[db].schemas {
		if matchPart(ep, schema) {
			o.addSchema(db, schema)
		}
	}
	return false, nil
}

func matchObjects(db dbKey, schema string, ep semantics.ObjExprPart, c *accountCache, o *AccountObjs) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.dbs[db]; !ok { return true, nil }
	c.dbs[db].mu.RLock()
	defer c.dbs[db].mu.RUnlock()
	if _, ok := c.dbs[db].schemas[schema]; !ok { return true, nil }
	if o.DBs[db].schemas[schema].Version == c.dbs[db].schemas[schema].version {
		// cache entry is stale
		err := c.refreshObjects(db, schema)
		if err != nil { return false, err } // TODO: if err is obj not exist then request retry
	}
	o.DBs[db].Schemas[schema].Version = c.dbs[db].schemas[schema].version
	for objKey := range o.DBs[db].Schemas[schema].Objects {
		if _, ok := c.dbs[db].schemas[schema].objects[objKey]; !ok {
			delete(o.DBs[db].Schemas[schema].Objects, objKey)
		}
	}
	for objKey := range c.dbs[db].schemas[schema].objects {
		if matchPart(ep, objKey.name) {
			o.addObject(db, schema, objKey)
		}
	}
	return false, nil
}

func (c *accountCache) refreshDBs() error {
	// Do not directly call this function, meant to be called only via match and friends,
	// which would have required appropriate write locks to mutexes
	dbNames, err := queryDBs()
	if err != nil { return err }
	for dbName, dbCache := range c.dbs {
		if _, ok := dbNames[dbName]; !ok {
			delete(c.dbs, dbName)
		} else {
			if dbCache.dbKind != dbNames[dbName] {
				delete(c.dbs, dbName)
			}
		}
	}
	for dbName, dbKind := range dbNames {
		if _, ok := c.dbs[dbName]; !ok {
			c.dbs[dbName] = newDBCache(dbName, dbKind)
		}
	}
	return nil
}

func queryDBs() (map[dbKey]true, error) {
	dbs := map[dbKey]string{}
	start := time.Now()
	log.Printf("Querying Snowflake for database names...\n")
	// TODO: consider how much work it would be to support APPLICATION DATABASE
	rows, err := getDB().Query(`SHOW TERSE DATABASES IN ACCOUNT ->> SELECT "name", "kind" FROM S1 WHERE "kind" IN ('STANDARD', 'IMPORTED DATABASE')`)
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

func (c *accountCache) refreshObjects(db DBID, schema string, schemaVersion int) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var dbC *dbCache
	if dbC, ok := c.dbs[db]; !ok { 
		// TODO: try and refresh account? and if successful, then try and refresh db?
		// And then if still not successful, return db does not exist error
		return
	} 
	dbC.mu.RLock()
	defer c.mu.RUnlock
	var schemaC *schemaCache
	if schemaC, ok := dbC.schemas[schema] {
		// return schema does not exist error
		// TODO: try and refresh DB?
		return
	}
	schemaC.mu.Lock() // NB! this is a write lock, since we are refreshing objects
	defer schemaC.mu.Unlock()
	if schemaVersion < schemaC.version {
		return schemaC.verison, nil // another thread may have already refreshed DB
	}
	// TODO: query objects, store in cache, and  bump version
	// if failure object does not exist, then call refreshSchema's; if that one fails because object does not exist; refresh databases
}
