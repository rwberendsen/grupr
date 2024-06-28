package snowflake

// couple of simple data structures to hold matched objects in account
type AccountObjs struct {
	Dbs map[string]DbObjs
}

type DbObjs struct {
	Schemas  map[string]SchemaObjs
	MatchAll bool
}

type SchemaObjs struct {
	// note that in case of drift during runtime tables and views may
	// contain the same keys (i.e., if during runtime a table was removed
	// and a view with the same name created)
	Tables   map[string]bool
	Views    map[string]bool
	MatchAll bool
}

func (o AccountObjs) addDB(db string, matchAllSchemas bool) AccountObjs {
	if _, ok := o.Dbs[db]; !ok {
		if o.Dbs == nil {
			o.Dbs = map[string]DbObjs{}
		}
		o.Dbs[db] = DbObjs{map[string]SchemaObjs{}, matchAllSchemas}
	}
	return o
}

func (o AccountObjs) addSchema(db string, schema string, matchAllTables bool) AccountObjs {
	if _, ok := o.Dbs[db].Schemas[schema]; !ok {
		o.Dbs[db].Schemas[schema] = SchemaObjs{map[string]bool{}, map[string]bool{}, matchAllTables}
	}
	return o
}

func (o AccountObjs) addTable(db string, schema string, obj string) AccountObjs {
	o.Dbs[db].Schemas[schema].Tables[obj] = true
	return o
}

func (o AccountObjs) addView(db string, schema string, obj string) AccountObjs {
	o.Dbs[db].Schemas[schema].Views[obj] = true
	return o
}

func (lhs AccountObjs) subtract(rhs AccountObjs) AccountObjs {
	r := AccountObjs{map[string]DbObjs{}}
	for k, v := range lhs.Dbs {
		if v2, ok := rhs.Dbs[k]; !ok {
			r.Dbs[k] = v
		} else {
			r.Dbs[k] = v.subtract(v2)
		}
	}
	return r
}

func (lhs DbObjs) subtract(rhs DbObjs) DbObjs {
	r := DbObjs{map[string]SchemaObjs{}, false}
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
	for k, _ := range lhs.Tables {
		if _, ok := rhs.Tables[k]; !ok {
			r.Tables[k] = true
		}
	}
	for k, _ := range lhs.Views {
		if _, ok := rhs.Views[k]; !ok {
			r.Views[k] = true
		}
	}
	return r
}

func (lhs AccountObjs) add(rhs AccountObjs) AccountObjs {
	if lhs.Dbs == nil {
		lhs.Dbs = map[string]DbObjs{}
	}
	for k, v := range rhs.Dbs {
		if _, ok := lhs.Dbs[k]; !ok {
			lhs.Dbs[k] = v
		} else {
			lhs.Dbs[k] = lhs.Dbs[k].add(rhs.Dbs[k])
		}
	}
	return lhs
}

func (lhs DbObjs) add(rhs DbObjs) DbObjs {
	lhs.MatchAll = lhs.MatchAll || rhs.MatchAll
	for k, v := range rhs.Schemas {
		if _, ok := lhs.Schemas[k]; !ok {
			lhs.Schemas[k] = v
		} else {
			lhs.Schemas[k] = lhs.Schemas[k].add(rhs.Schemas[k])
		}
	}
	return lhs
}

func (lhs SchemaObjs) add(rhs SchemaObjs) SchemaObjs {
	lhs.MatchAll = lhs.MatchAll || rhs.MatchAll
	for k, _ := range rhs.Tables {
		if _, ok := lhs.Tables[k]; !ok {
			lhs.Tables[k] = true
		}
	}
	for k, _ := range rhs.Views {
		if _, ok := lhs.Views[k]; !ok {
			lhs.Views[k] = true
		}
	}
	return lhs
}
