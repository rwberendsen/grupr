package state

// couple of simple data structures to hold matched objects in account
type accountObjs struct {
	dbs map[string]dbObjs
}

type dbObjs struct {
	schemas  map[string]schemaObjs
	matchAll bool
}

type schemaObjs struct {
	// note that in case of drift during runtime tables and views may
	// contain the same keys (i.e., if during runtime a table was removed
	// and a view with the same name created)
	tables   map[string]bool
	views    map[string]bool
	matchAll bool
}

func (o accountObjs) addDB(db string, matchAllSchemas bool) accountObjs {
	if _, ok := o.dbs[db]; !ok {
		if o.dbs == nil {
			o.dbs = map[string]dbObjs{}
		}
		o.dbs[db] = dbObjs{map[string]schemaObjs{}, matchAllSchemas}
	}
	return o
}

func (o accountObjs) addSchema(db string, schema string, matchAllTables bool) accountObjs {
	if _, ok := o.dbs[db].schemas[schema]; !ok {
		o.dbs[db].schemas[schema] = schemaObjs{map[string]bool{}, map[string]bool{}, matchAllTables}
	}
	return o
}

func (o accountObjs) addObject(db string, schema string, obj string, t dbType) accountObjs {
	if t == _table {
		o.dbs[db].schemas[schema].tables[obj] = true
	}
	if t != _view {
		panic("unsupported dbType value")
	}
	o.dbs[db].schemas[schema].views[obj] = true
	return o
}

func (lhs accountObjs) subtract(rhs accountObjs) accountObjs {
	r := accountObjs{map[string]dbObjs{}}
	for k, v := range lhs.dbs {
		if v2, ok := rhs.dbs[k]; !ok {
			r.dbs[k] = v
		} else {
			r.dbs[k] = v.subtract(v2)
		}
	}
	return r
}

func (lhs dbObjs) subtract(rhs dbObjs) dbObjs {
	r := dbObjs{map[string]schemaObjs{}, false}
	for k, v := range lhs.schemas {
		if v2, ok := rhs.schemas[k]; !ok {
			r.schemas[k] = v
		} else {
			r.schemas[k] = v.subtract(v2)
		}
	}
	return r
}

func (lhs schemaObjs) subtract(rhs schemaObjs) schemaObjs {
	r := schemaObjs{map[string]bool{}, map[string]bool{}, false}
	for k, _ := range lhs.tables {
		if _, ok := rhs.tables[k]; !ok {
			r.tables[k] = true
		}
	}
	for k, _ := range lhs.views {
		if _, ok := rhs.views[k]; !ok {
			r.views[k] = true
		}
	}
	return r
}

func (lhs accountObjs) add(rhs accountObjs) accountObjs {
	if lhs.dbs == nil {
		lhs.dbs = map[string]dbObjs{}
	}
	for k, v := range rhs.dbs {
		if _, ok := lhs.dbs[k]; !ok {
			lhs.dbs[k] = v
		} else {
			lhs.dbs[k] = lhs.dbs[k].add(rhs.dbs[k])
		}
	}
	return lhs
}

func (lhs dbObjs) add(rhs dbObjs) dbObjs {
	lhs.matchAll = lhs.matchAll || rhs.matchAll
	for k, v := range rhs.schemas {
		if _, ok := lhs.schemas[k]; !ok {
			lhs.schemas[k] = v
		} else {
			lhs.schemas[k] = lhs.schemas[k].add(rhs.schemas[k])
		}
	}
	return lhs
}

func (lhs schemaObjs) add(rhs schemaObjs) schemaObjs {
	lhs.matchAll = lhs.matchAll || rhs.matchAll
	for k, _ := range rhs.tables {
		if _, ok := lhs.tables[k]; !ok {
			lhs.tables[k] = true
		}
	}
	for k, _ := range rhs.views {
		if _, ok := lhs.views[k]; !ok {
			lhs.views[k] = true
		}
	}
	return lhs
}
