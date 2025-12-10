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
	for db, matchedDBObjects := range p.matchedAccountObjects[e].dbs {
		if p.matchedAccountObjects[e].hasDB(db) {
			dbExcluded := false
			for excludeExpr := range om.Exclude {
				if excludeExpr.MatchesAllObjectsInDB(db.Name) {
					dbExcluded = true
				}
			}
			if !dbExcluded {
				p.AccountObjects[e].addDB(db)
				for schema, matchedSchemaObjects := range matchedDBObjects.schemas {
					if matchedDBObjects.hasSchema(schema) {
						schemaExcluded := false
						for excludeExpr := range om.Exclude {
							if excludeExpr.MatchesAllObjectsInSchema(db.Name, schema) {
								schemaExcluded = true
							}
						}
						if !schemaExcluded {
							p.AccountObjects[e].DBs[db].addSchema(schema)
							// WIP
						}
					}
				}
			}
		}
	}
}

func (o *AccountObjs) addDB(db string) {
	if o.DBs == nil {
		o.DBs = map[string]*DBObjs{}
	}
	if _, ok := o.DBs[db]; !ok {
		o.DBs[db] = &DBObjs{}
	}
}

func (lhs AccountObjs) subtract(rhs AccountObjs) AccountObjs {
	r := AccountObjs{map[string]DBObjs{}}
	for k, v := range lhs.DBs {
		if v2, ok := rhs.DBs[k]; !ok {
			r.DBs[k] = v
		} else {
			r.DBs[k] = v.subtract(v2)
		}
	}
	return r
}

func (lhs DBObjs) subtract(rhs DBObjs) DBObjs {
	r := DBObjs{map[string]SchemaObjs{}, false}
	for k, v := range lhs.Schemas {
		if v2, ok := rhs.Schemas[k]; !ok {
			r.Schemas[k] = v
		} else {
			r.Schemas[k] = v.subtract(v2)
		}
	}
	return r
}

func (lhs SchemaObjs) subtract(rhs SchemaObjs) SchemaObjs {
	r := SchemaObjs{map[string]bool{}, map[string]bool{}, false}
	for k := range lhs.Tables {
		if _, ok := rhs.Tables[k]; !ok {
			r.Tables[k] = true
		}
	}
	for k := range lhs.Views {
		if _, ok := rhs.Views[k]; !ok {
			r.Views[k] = true
		}
	}
	return r
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
