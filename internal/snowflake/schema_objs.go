package snowflake

type SchemaObjs struct {
	Objects map[ObjKey]struct
	MatchAllObjects bool
}

func newSchemaObjsFromMatched(m *matchedSchemaObjs) *SchemaObjs {
	o := &SchemaObjs{Objects: map[ObjKey]struct{},}
	for k := range m.objects {
		o.objects[k] = struct{}
	}
}

func newSchemaObjs(db DBKey, schema string, o *SchemaObjs, e semantics.ObjExpr, om semantics.ObjMatcher) *SchemaObjs {
	r := &SchemaObjs{Objects: map[ObjKey]struct{},}
	r.setMatchAllObjects(db, e, om)
	for objKey := range o.Objects {
		if !e[semantics.Object].Match(objKey.Name) { continue }
		for excludeExpr := range om.Exclude {
			if excludeExpr.Match(db.Name, schema, objKey.Name) {
				continue
			}
		}
		o.Objects[objKey] = struct{}
	}
}

func (o *SchemaObjs) setMatchAllObjects(db DBKey, schema string, e semantics.ObjExpr, om semantics.ObjMatcher) {
	if !e[semantics.Object].MatchAll() { return }
	o.MatchAllObjects = true
	for excludeExpr := range om.Exclude {
		if excludeExpr[semantics.Database].Match(db.Name) && excludeExpr[semantics.Schema].Match(schema) {
			o.MatchAllObjects = false
		}
	}
}
