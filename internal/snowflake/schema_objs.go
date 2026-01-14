package snowflake

type SchemaObjs struct {
	Objects map[string]ObjAttr // no need for pointers at this lowest level
	MatchAllObjects bool
	GrantsTo map[Mode]map[Privilege]struct{}
}

func newSchemaObjs(db string, schema string, o *SchemaObjs, om semantics.ObjMatcher) *SchemaObjs {
	r := &SchemaObjs{Objects: map[string]ObjAttr{},}
	r.setMatchAllObjects(db, om)
	for k, v := range o.Objects {
		if !om.DisjointFromObject(db.Name, schema, k) {
			// Note how we create a new ObjAttr here; 
			// similar to how we create new SchemaObjs and DBObjs;
			// we do not want to share GrantsTo between them.
			r.Objects[k] = ObjAttr{ObjectType: v.ObjectType, Owner: v.Owner,}
		}
	}
}

func newSchemaObjsFromMatched(m *matchedSchemaObjs) *SchemaObjs {
	r := &SchemaObjs{Objects: map[string]ObjAttr{},}
	for k, v := range m.objects {
		r.Objects[k] = v // GrantsTo == nil on matchedAccountObjs anyway
	}
}

func (o *SchemaObjs) setMatchAllObjects(db string, schema string, om semantics.ObjMatcher) {
	if om.SupersetOfSchema(db.Name, schema) {
		o.MatchAllObjects = true
	}
}

func (o *SchemaObjs) hasObject(k string) bool {
	_, ok := o.Objects[k]
	return ok
}

func (o *SchemaObjs) setGrantTo(m Mode, p Privilege) {
	if o.GrantsTo == nil { o.GrantsTo = map[Mode]map[Privilege]struct{}{} }
	if _, ok := o.GrantsTo[m]; !ok { o.GrantsTo[m] = map[Privilege]struct{}{} }
	o.GrantsTo[m][p] = struct{}{}
}
