package snowflake

type SchemaObjs struct {
	Objects map[ObjKey]struct{}
	MatchAllObjects bool
}

func newSchemaObjs(db DBKey, schema string, o *SchemaObjs, om semantics.ObjMatcher) *SchemaObjs {
	r := &SchemaObjs{Objects: map[ObjKey]struct{}{},}
	r.setMatchAllObjects(db, om)
	for objKey := range o.Objects {
		if !om.DisjointFromObject(db.Name, schema, objKey.Name) {
			o.Objects[objKey] = struct{}{}
		}
	}
}

func newSchemaObjsFromMatched(m *matchedSchemaObjs) *SchemaObjs {
	o := &SchemaObjs{Objects: map[ObjKey]struct{}{},}
	for k := range m.objects {
		o.objects[k] = struct{}{}
	}
}

func (o *SchemaObjs) setMatchAllObjects(db DBKey, schema string, om semantics.ObjMatcher) {
	if om.SupersetOfSchema(db.Name, schema) {
		o.MatchAllObjects = true
	}
}
