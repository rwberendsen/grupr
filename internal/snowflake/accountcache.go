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
	mu	sync.Mutex // guards dbs and version
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

func (c *accountCache) getDBs(accountVersion int) (map[string]*dbCache, int, error) {
	// Thread-safe method to get databases in an account
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dbs == nil { c.dbs = map[string]*dbCache{} }
	// below check is done because another thread may have already refreshed,
	// in which case we don't need to go and fetch databases again
	if accountVersion < c.version {
		return c.dbs, c.version, nil
	}
	err := c.addDBs()
	if err != nil { return c.dbs, c.version, err }
	c.version += 1
	return c.dbs, c.version, nil
}

func (c *accountCache) addDBs() error {
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
