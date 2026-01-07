package snowflake

type SchemaObjs struct {
	Objects map[ObjKey]struct{}
	MatchAllObjects bool
}

func newSchemaObjs(db string, schema string, o *SchemaObjs, om semantics.ObjMatcher) *SchemaObjs {
	r := &SchemaObjs{Objects: map[ObjKey]struct{}{},}
	r.setMatchAllObjects(db, om)
	for k := range o.Objects {
		if !om.DisjointFromObject(db.Name, schema, k.Name) {
			r.Objects[k] = struct{}{}
		}
	}
}

func newSchemaObjsFromMatched(m *matchedSchemaObjs) *SchemaObjs {
	r := &SchemaObjs{Objects: map[ObjKey]struct{}{},}
	for k := range m.objects {
		r.Objects[k] = struct{}{}
	}
}

func (o *SchemaObjs) setMatchAllObjects(db string, schema string, om semantics.ObjMatcher) {
	if om.SupersetOfSchema(db.Name, schema) {
		o.MatchAllObjects = true
	}
}
