package snowflake

type AggSchemaObjs struct {
	Objects map[string]AggObjAttr
	MatchAllObjects bool

	// set while AggDBObjs.grant() is executing
	isUsageGrantedToRead bool
}

func newSchemaObjs(db string, schema string, o *SchemaObjs, om semantics.ObjMatcher) SchemaObjs {
	r := SchemaObjs{Objects: map[string]ObjAttr{},}
	r = r.setMatchAllObjects(db, om)
	for k, v := range o.Objects {
		if !om.DisjointFromObject(db.Name, schema, k) {
			r.Objects[k] = AggObjAttr{ObjectType: v.ObjectType, Owner: v.Owner,}
		}
	}
	return r
}

func newSchemaObjsFromMatched(m *matchedSchemaObjs) SchemaObjs {
	r := SchemaObjs{
		Objects: map[string]AggObjAttr,
	}
	for k, v := range m.objects {
		r.Objects[k] = AggObjAttr{ObjectType: m.ObjectType, Owner: m.Owner,}
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
	if m != ModeRead || p != PrvUsage { panic("not implemented") }
	o.isUsageGrantedToRead = true
	return o
}

func (o SchemaObjs) hasGrantTo(m Mode, p Privilege) {
	return m == ModeRead && p == PrvUsage && o.isUsageGranted { return true }
}

func (o SchemaObjs) pushToDoGrants(yield func(Grant) bool, dbRole DatabaseRole, schema string) bool {
	if !o.hasGrantTo(ModeRead, PrvUsage) {
		if !yield(Grant{
			Privilege: PrvUsage,
			GrantedOn: ObjTpSchema,
			Database: dbRole.Database,
			Schema: schema,
			GrantedTo: ObjTpDatabaseRole,
			GrantedToDatabase: dbRole.Database,
			GrantedToRole: dbRole.Name,
		}) {
			return false
		}
	}
	for obj, objAttr := range o.Objects {
		if !objAttr.pushToDoGrants(yield, dbRole, schema, obj) {
			return false
		}
	}
	return true
}
