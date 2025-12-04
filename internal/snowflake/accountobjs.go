package snowflake

// Couple of simple data structures to hold matched objects in account
type AccountObjs struct {
	Version int // version with regard to accountCache
	DBs map[dbKey]*DBObjs
	MatchAllDBs bool
	MatchAllSchemas bool
	MatchAllObjects bool
}

func newAccountObjs(ctx context.Context, conn *sql.DB, m semantics.ObjMatcher, c *accountCache) (*AccountObjs, error) {
	o := &AccountObjs{}
	if err := c.match(ctx, conn, m.Include, o); err != nil {
		return nil, err
	}
	o.setMatchAll(m)
	o.processExcludes(m)
	return o, nil
}

func (o *AccountObjs) setMatchAll(m semantics.ObjMatcher) {
	// evaluate m.Include, as well as all excludes about how the MatchAll attributes need to be set.
	// For example: "a.*.*", excluding "a.b.*" should no longer match all schemas, cause schema b
	// is entirely excluded.
}

func (o *AccountObjs) processExcludes(m semantics.ObjMatcher) {
	// walk over all excludes, and remove excluded objects from o
}

func (o *AccountObjs) addDB(db string) {
	if _, ok := o.DBs[db]; !ok {
		if o.DBs == nil {
			o.DBs = map[string]*DBObjs{}
		}
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
