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
	dbs     map[string]*dbCache // nil: never requested; empty: none found
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

func (c *accountCache) matchDBs(e semantics.ExprPart, accountVersion int) (map[string]bool, fresh bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if accountVersion >= c.version { return }
	matchedDBS := matchPart(e, c.dbs) // TODO: also return kind of DB?
	fresh = true
	return
}

func (c* accountCache) matchSchemas(e semantics.ExprPart, db DBID, dbVersion int) (map[string]bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var dbc *dbCache
	if dbc, ok := c.dbs[db]; !ok { 
		// return db does not exist error
		return
	} 
	dbc.mu.RLock()
	defer c.mu.RUnlock
	if dbVersion >= dbc.version {
		// return not fresh error
		return
	}
	matchedSchemas := matchPart(e, dbc.schemas)
	return
}

func (c* accountCache) matchObjects(e semantics.ExprPart, db DBID, schema string, schemaVersion int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var dbC *dbCache
	if dbC, ok := c.dbs[db]; !ok { 
		// return db does not exist error
		return
	} 
	dbC.mu.RLock()
	defer c.mu.RUnlock
	var schemaC *schemaCache
	if schemaC, ok := dbC.schemas[schema] {
		// return schema does not exist error
		return
	}
	schemaC.mu.RLock()
	defer schemaC.mu.RUnlock()
	if schemaVersion >= schemaC.version {
		// return not fresh error
		// TODO: ok indeed to skip checking db version?
		return
	}
	matchedObjects := matchPart(e, schemaC.objects) // TODO: also return kind of object?
	return
}

func (c *accountCache) getDBs(accountVersion int) (map[string]bool, int, error) {
	// Thread-safe method to get databases in an account
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dbs == nil { c.dbs = map[string]*dbCache{} }
	// below check is done because another thread may have already refreshed,
	// in which case we don't need to go and fetch databases again
	if accountVersion < c.version {
		return c.dbs, c.version, nil
	}
	err := c.refreshDBs()
	if err != nil { return c.dbs, c.version, err }
	c.version += 1
	return c.dbs, c.version, nil
}

func (c *accountCache) refreshDBs() error {
	// Do not directly call this function, meant to be called only from dbCache.getSchemas
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

func queryDBs() (map[string]string, error) {
	dbs := map[string]string{}
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
		if _, ok := dbs[dbName]; ok { return nil, fmt.Errorf("duplicate db name: %s", dbName) }
		dbs[dbName] = dbKind
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("queryDBs: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for database names took %v\n", t.Sub(start))
	return dbs, nil
}

func (c *accountCache) refreshObjects(db DBID, schema string, schemaVersion int) int, error {
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
}
