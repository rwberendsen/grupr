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
	mu		sync.Mutex // guards objects and version
	version int
	objects		map[objKey]struct{} // nil: never requested; empty: none present; value: TABLE or VIEW
	tables		map[string]string // value is owner
	views		map[string]string // value is owner
}

func (c *schemaCache) refreshObjects(ctx context.Context, conn *sql.DB, dbName string, schemaName string) error {
	// intended to be called from accountCache.match and friends, which will acquire locks on the appropriate mutexes
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
	// TODO: when there are more than 10K results, paginate
	rows, err := getDB().Query(`SHOW TERSE OBJECTS IN SCHEMA IDENTIFIER(?) ->> SELECT "name", "kind" FROM S1`, quoteIdentifier(dbName) + "." + quoteIdentifier(schemaName))
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

func (c *schemaCache) queryTables(ctx context.Context, conn *sql.DB, dbName string, schemaName string) error {
	// when there are more than 10K results, paginate.
	// because we apply filters, even if fewer results are returned, perhaps there are still more
	// for that reason, our last row has a count of the first query result
	mayHaveMore := true
	var lastName string
	var fromClause string
	limit := 10000
	for mayHaveMore {
		start := time.Now()
		log.Printf("Querying Snowflake for object names in schema: %s.%s ...\n", dbName, schemaName)
		rows, err := getDB().Query(fmt.Sprintf(`SHOW TABLES IN SCHEMA IDENTIFIER(?) LIMIT %d%s ->>
SELECT NULL AS n, "name" AS name, "owner" AS owner FROM S1 WHERE "kind" in ('TABLE', 'TRANSIENT') ->>
SELECT n, name, owner FROM $2
UNION ALL
SELECT COUNT(*), '' AS name, '' AS owner FROM $1`, limit, fromClause), dbName + "." + schemaName)
		if err != nil {
			if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
				return ErrObjectNotExistOrAuthorized
			}
			return fmt.Errorf("queryObjects error: %w", err)
		}
		for rows.Next() {
			var n *int
			var name string
			var owner string
			if err = rows.Scan(n, &name, &owner); err != nil {
				return fmt.Errorf("queryTables: error scanning row: %w", err)
			}
			if n != nil { // this is the last row holding the count
				if n < limit {
					mayHaveMore = false
				} else {
					fromClause = fmt.Sprintf(" FROM '%s'", lastName)
				}
				continue
			}
			if _, ok := c.tables[name]; ok { return nil, fmt.Errorf("duplicate table: %v", name) }
			lastName = name
			c.tables[name] = owner
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("queryObjects: error after looping over results: %w", err)
		}
		t := time.Now()
		log.Printf("Querying Snowflake for object names in schema: %s.%s took %v\n", dbName, schemaName, t.Sub(start))
	}
	return objects, nil
}

