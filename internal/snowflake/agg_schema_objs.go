package snowflake

type AggSchemaObjs struct {
	Objects map[string]AggObjAttr
	MatchAllObjects bool
	GrantsTo map[Mode]map[Privilege]struct{}
}

func newSchemaObjs(db string, schema string, o *SchemaObjs, om semantics.ObjMatcher) SchemaObjs {
	r := SchemaObjs{Objects: map[string]ObjAttr{},}
	r = r.setMatchAllObjects(db, om)
	for k, v := range o.Objects {
		if !om.DisjointFromObject(db.Name, schema, k) {
			// Note how we create a new ObjAttr here; 
			// similar to how we create new SchemaObjs and DBObjs;
			// we do not want to share GrantsTo between them.
			r.Objects[k] = ObjAttr{ObjectType: v.ObjectType, Owner: v.Owner,}
		}
	}
	return r
}

func newSchemaObjsFromMatched(m *matchedSchemaObjs) SchemaObjs {
	r := SchemaObjs{
		Objects: m.objects,
	}
	m.objects = nil // no need to retain all objects in memory, only schema version will do
	return r
}

func (o SchemaObjs) setMatchAllObjects(db string, schema string, om semantics.ObjMatcher) SchemaObjs {
	if om.SupersetOfSchema(db.Name, schema) {
		o.MatchAllObjects = true
	}
	return o
}

func (o SchemaObjs) hasObject(k string) bool {
	_, ok := o.Objects[k]
	return ok
}

func (lhs SchemaObjs) add(rhs SchemaObjs) SchemaObjs {
	// Note that when we add together SchemaObjs, we do so within an interface,
	// where all ObjExpr are known to be disjoint from each other.
	// Therefore we do not have to worry about different ObjAttr for the same key
	if lhs.Objects == nil {
		lhs.Objects = map[string]ObjAttr{}
	}
	for k, v := range rhs.Objects {
		lhs.Objects[k] = v
	}
	lhs.MatchAllObjects = lhs.MatchAllObjects || rhs.MatchAllObjects
	return lhs
}

func (o SchemaObjs) setGrantTo(m Mode, p Privilege) SchemaObjs {
	if o.GrantsTo == nil { o.GrantsTo = map[Mode]map[Privilege]struct{}{} }
	if _, ok := o.GrantsTo[m]; !ok { o.GrantsTo[m] = map[Privilege]struct{}{} }
	o.GrantsTo[m][p] = struct{}{}
	return o
}

func (o SchemaObjs) hasGrantTo(m Mode, p Privilege) {
	if v, ok := o.GrantsTo[m] {
		_, ok = v[p]
		return ok
	}
}

func (o SchemaObjs) doGrant(ctx context.Context, cnf *Config, conn *sql.DB, db string, schema string, role string) error {
	// WIP
	if !o.hasGrantTo(ModeRead, PrvUsage) {
		if err := GrantToRole{
				Privilege: PrvUsage,
				GrantedOn: ObjTpSchema,
				Database: db,
				Schema: schema,
		}.DoGrantToDBRole(ctx, cnf, conn, db, role); err != nil {
			return err
		}
	}
	for obj, objAttr := range o.Objects {
		if err := objAttr.doGrant(ctx, cnf, conn, db, schema, obj, role); err != nil {
			return err
		}
	}
	return nil
}
