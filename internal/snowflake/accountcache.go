package snowflake

import (
	"context"
	"fmt"
	"strings"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/snowflakedb/gosnowflake"
)

// caching objects in Snowflake locally
type accountCache struct {
	mu	sync.RWMutex // guards dbs, dbExists, and version
	version int
	dbs     map[DBKey]*dbCache // nil: never requested; empty: none found
	dbExists map[DBKey]bool
}

// TODO: move this to some file with global functions
func escapeIdentifier(s string) string {
	return strings.ReplaceAll(s, "\"", "\"\"")
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func (c *accountCache) match(ctx context.Context, conn *sql.DB, e semantics.ObjExpr, o *matchedAccountObjs) error {
	// will modify both c and o
	err := c.matchDBs(ctx, e[semantics.Database], o)
	if err != nil { return err }
	for db, dbObjs := range o.dbs {
		if !o.hasDB(db) { continue }
		err := c.matchSchemas(ctx, conn, db, e, dbObjs)
		if err != nil { return err }
		for schema, schemaObjs := range dbObjs.schemas {
			if !dbObjs.hasSchema(schema) { continue }
			err = c.matchObjects(ctx, conn, db, schema, e, schemaObjs)
			if err != nil { return err }
		}
	}
}

func (c *accountCache) matchDBs(ctx context.Context, conn *sql.DB, ep semantics.ObjExprPart, o *matchedAccountObjs) error {
	c.mu.Lock() // block till all another writer or any active readers are done, get a write lock, now you are the only one modifying the tree
	defer c.mu.Unlock()
	if o.version == c.version {
		// cache entry is stale
		err := c.refreshDBs(ctx, conn)
		if err != nil { return err }
	}
	o.version = c.version
	for k, _ := range o.dbs {
		if !o.hasDB(k) { continue }
		if !c.hasDB(k) {
			o.dropDB(k)
		}
	}
	for k := range c.dbs {
		if !c.hasDB(k) { continue }
		if ep.Match(k.Name) {
			o.addDB(k)
		}
	}
	return nil
}

func (c *accountCache) matchSchemas(ctx context.Context, conn *sql.DB, db DBKey, ep semantics.ObjExprPart, o *matchedDBObjs) error {
	c.mu.RLock() // Block till a (requesting) writer (obtains and) releases the lock, if any, get a read lock, now you can read this node, 
		     // concurrently with other readers
	defer c.mu.RUnlock()
	if _, ok := c.dbs[db]; !ok {
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
		err := c.dbs[db].refreshSchemas(ctx, conn, db.Name)
		if err != nil { return err }
	}
	o.version = c.dbs[db].version
	for k, _ := range o.schemas {
		if !o.hasSchema(k) { continue }
		if !c.dbs[db].hasSchema(k) {
			o.dropSchema(k)
		}
	}
	for k := range c.dbs[db].schemas {
		if !c.dbs[db].hasSchema(k) { continue }
		if ep.Match(k) {
			o.addSchema(k)
		}
	}
	return false, nil
}

func (c *accountCache) matchObjects(ctx context.Context, conn *sql.DB, db DBKey, schema string, ep semantics.ObjExprPart, o *matchedSchemaObjs) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.dbs[db]; !ok { return ErrObjectNotExistOrAuthorized }
	c.dbs[db].mu.RLock()
	defer c.dbs[db].mu.RUnlock()
	if _, ok := c.dbs[db].schemas[schema]; !ok { return ErrObjectNotExistOrAuthorized }
	c.dbs[db].schemas[schema].mu.Lock() // get a write lock on this schema
	defer c.dbs[db].schemas[schema].mu.Unlock()
	if o.version == c.dbs[db].schemas[schema].version {
		// cache entry is stale
		err := c.refreshObjects(ctx, conn, db.Name, schema)
		if err != nil { return err }
	}
	o.version = c.dbs[db].schemas[schema].version
	o.objects = map[ObjKey]struct{}
	for k := range c.dbs[db].schemas[schema].objects {
		if ep.Match(k.Name) {
			o.objects[k] = struct{}
		}
	}
	return false, nil
}

func (c *accountCache) refreshDBs(ctx context.Context, conn *sql.DB) error {
	// Do not directly call this function, meant to be called only via match and friends,
	// which would have required appropriate write locks to mutexes
	dbs, err := queryDBs(ctx, conn)
	if err != nil { return err }
	c.version += 1
	for k, v := range c.dbs {
		if !c.hasDB(k) { continue }
		if _, ok := dbs[k]; !ok {
			c.dropDB(k)
		}
	}
	for k := range dbs {
		c.addDB(k)
	}
	return nil
}

func (c *accountCache) addDB(k DBKey) {
	if c.dbs == nil {
		c.dbs = map[DBKey]*dbCache{}
		c.dbExists = map[DBKey]bool{}
	}
	if _, ok := c.dbs[k]; !ok {
		c.dbs[k] = &dbCache{}
	}
	c.dbExists[k] = true
}

func (c *accountCache) dropDB(k DBKey) {
	if _, ok := c.dbs[k]; !ok {
		panic(fmt.Sprintf("DBKey not found: '%s'", k))
	}
	c.dbExists[k] = false
}

func (c *accountCache) hasDB(k DBKey) bool {
	return c.dbExists != nil && c.dbExists[k]
}

func queryDBs(ctx context.Context, conn *sql.DB) (map[DBKey]struct, error) {
	dbs := map[DBKey]struct{}
	start := time.Now()
	log.Printf("Querying Snowflake for database names...\n")
	// TODO: consider how much work it would be to support APPLICATION DATABASE
	// TODO: when there are more than 10K results, paginate
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
		db := DBKey{dbName, dbKind}
		if _, ok := dbs[db]; ok { return nil, fmt.Errorf("duplicate db: %v", db) }
		dbs[db] = struct
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("queryDBs: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for database names took %v\n", t.Sub(start))
	return dbs, nil
}
