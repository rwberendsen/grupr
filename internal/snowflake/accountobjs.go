package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)


// Couple of simple data structures to hold matched objects in account
type AccountObjs struct {
	DBs map[string]*DBObjs
}

func newAccountObjsFromMatched(m *matchedAccountObjs) *AccountObjs {
	o := &AccountObjs{DBS: map[string]*DBObjs{},}
	for k, v := range m.getDBs() {
		o.DBs[k] = newDBObjsFromMatched(v)
	}
	return o
}

func newAccountObjs(o *AccountObjs, om semantics.ObjMatcher) *AccountObjs {
	r := &AccountObjs{DBs: map[string]*DBObjs{},}
	for db, dbObjects := range o.DBs {
		if !om.DisjointFromDB(db.Name) {
			r.DBs[db] = newDBObjs(db, dbObjects, om)
		}
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
