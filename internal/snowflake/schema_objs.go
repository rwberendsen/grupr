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

func (o *SchemaObjs) addObject(k objKey) {
	if _, ok := o.Objects[k]; !ok {
		if o.Objects == nil {
			o.Objects = map[objKey]struct{}
		}
		o.Objects[k] = struct{}
	}
}
