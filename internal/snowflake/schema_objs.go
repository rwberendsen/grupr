package snowflake

type SchemaObjs struct {
	Objects map[string]ObjAttr
	MatchAllObjects bool
}

func newSchemaObjs(db string, schema string, o SchemaObjs, om semantics.ObjMatcher) SchemaObjs {
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
	m.objects = nil // no need to retain; note that we create an AccountObjs from a matchedAccountObjs only once.
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

func (o SchemaObjs) countByObjType(t ObjType) int {
	r := 0
	for _, v := range o.Objects {
		if o.ObjectType == t {
			r += 1
		}
	}
	return r
}

func (lhs SchemaObjs) add(rhs SchemaObjs) SchemaObjs {
	// Note that when we add together SchemaObjs, we do so within an interface,
	// where all ObjExpr are known to be disjoint from each other.
	// Therefore we do not have to worry about different ObjAttr for the same key
	if lhs.Objects == nil {
		return rhs
	}
	for k, v := range rhs.Objects {
		lhs.Objects[k] = v
	}
	lhs.MatchAllObjects = lhs.MatchAllObjects || rhs.MatchAllObjects
	return lhs
}
