package snowflake

// couple of simple data structures to hold matched objects in account
type AccountObjs struct {
	DBs map[string]DBObjs
}

type DBObjs struct {
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
	if _, ok := o.DBs[db]; !ok {
		if o.DBs == nil {
			o.DBs = map[string]DBObjs{}
		}
		o.DBs[db] = DBObjs{map[string]SchemaObjs{}, matchAllSchemas}
	}
	return o
}

func (o AccountObjs) addSchema(db string, schema string, matchAllTables bool) AccountObjs {
	if _, ok := o.DBs[db].Schemas[schema]; !ok {
		o.DBs[db].Schemas[schema] = SchemaObjs{map[string]bool{}, map[string]bool{}, matchAllTables}
	}
	return o
}

func (o AccountObjs) addTable(db string, schema string, obj string) AccountObjs {
	o.DBs[db].Schemas[schema].Tables[obj] = true
	return o
}

func (o AccountObjs) addView(db string, schema string, obj string) AccountObjs {
	o.DBs[db].Schemas[schema].Views[obj] = true
	return o
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
