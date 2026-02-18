package snowflake

import (
	"context"
	"database/sql"
	"sync"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type schemaCache struct {
	mu      sync.Mutex // guards objects and version
	version int
	objects map[semantics.Ident]ObjAttr // nil: never requested; empty: none present;
}

func (c *schemaCache) refreshObjects(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) error {
	// intended to be called from accountCache.match and friends, which will acquire locks on the appropriate mutexes
	c.objects = map[semantics.Ident]ObjAttr{} // overwrite if it had a value
	for obj, err := range QueryObjs(ctx, conn, db, schema) {
		if err != nil {
			return err
		}
		c.objects[obj.Name] = ObjAttr{ObjectType: obj.ObjectType, Owner: obj.Owner}
	}
	c.version += 1
	return nil
}
