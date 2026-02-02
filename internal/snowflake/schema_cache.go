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
	mu      sync.Mutex // guards objects and version
	version int
	objects map[string]ObjAttr // nil: never requested; empty: none present;
}

func (c *schemaCache) refreshObjects(ctx context.Context, conn *sql.DB, dbName string, schemaName string) error {
	// intended to be called from accountCache.match and friends, which will acquire locks on the appropriate mutexes
	c.objects = map[string]Obj{} // overwrite if it had a value
	for obj, err := range QueryObjs(ctx, conn, dbName, schemaName) {
		if err != nil {
			return err
		}
		c[obj.Name] = ObjAttr{ObjectType: obj.ObjectType, Owner: obj.Owner}
	}
	c.version += 1
	return nil
}
