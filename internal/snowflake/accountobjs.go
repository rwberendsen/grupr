package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)


// Couple of simple data structures to hold matched objects in account
type AccountObjs struct {
	DBs map[DBKey]*DBObjs
}

func newAccountObjsFromMatched(m *matchedAccountObjs) *AccountObjs {
	o := &AccountObjs{DBS: map[DBKey]*DBObjs{},}
	for k, v := range m.dbs {
		if !m.hasDB(k) { continue }
		o.DBs[k] = newDBObjsFromMatched(v)
	}
	return o
}

func newAccountObjs(o *AccountObjs, e semantics.ObjExpr, om semantics.ObjMatcher) *AccountObjs {
	r := &AccountObjs{DBs: map[DBKey]*DBObjs{},}
	for db, dbObjects := range o.DBs {
		if !e[semantics.Database].Match(db.Name) { continue }
		for excludeExpr := range om.Exclude {
			if excludeExpr.MatchesAllObjectsInDB(db.Name) {
				continue
			}
		}
		r.DBs[db] = newDBObjs(db, dbObjects, e, om)
	}
}

func (o AccountObjs) TableCount() int {
	r := 0
	for _, db := range o.DBs {
		for _, schema := range db.Schemas {
			r += len(schema.Tables)
		}
	}
	return r
}

func (o AccountObjs) ViewCount() int {
	r := 0
	for _, db := range o.DBs {
		for _, schema := range db.Schemas {
			r += len(schema.Views)
		}
	}
	return r
}
