package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)


// Couple of simple data structures to hold matched objects in account
type AccountObjs struct {
	DBs map[string]DBObjs
}

func newAccountObjs(o *AccountObjs, om semantics.ObjMatcher) *AccountObjs {
	r := &AccountObjs{DBs: map[string]*DBObjs{},}
	for db, dbObjects := range o.DBs {
		if !om.DisjointFromDB(db) {
			r.DBs[db] = newDBObjs(db, dbObjects, om)
		}
	}
	return r
}

func newAccountObjsFromMatched(m *matchedAccountObjs) *AccountObjs {
	r := &AccountObjs{DBS: map[string]*DBObjs{},}
	for k, v := range m.getDBs() {
		r.DBs[k] = newDBObjsFromMatched(v)
	}
	return r
}

func (lhs AccountObjs) add(rhs AccountObjs) AccountObjs {
	if lhs.DBs == nil { // not an expected scenario, but does not hurt
		lhs.DBs = map[string]DBObjs{}
	}
	for k, v := range rhs.DBs {
		lhs.DBs[k] = lhs.DBs[k].add(v)
	}
	return lhs
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
