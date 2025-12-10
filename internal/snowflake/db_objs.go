package snowflake

type DBObjs struct {
	Schemas  map[string]*SchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool
}

func newDBObjsFromMatched(m *matchedDBObjs) *DBObjs {
	o := &DBObjs{Schemas: map[string]*SchemaObjs{},}
	for k, v := range m.schemas {
		if !m.hasSchema(k) { continue }
		o.Schemas[k] = newSchemaObjsFromMatched(v)
	}
	return o
}

func (o *DBObjs) addSchema(schema string) {
	if _, ok := o.Schemas[schema]; !ok {
		if o.Schemas == nil {
			o.Schemas = map[string]*SchemaObjs{}
		}
		o.Schemas[schema] = &SchemaObjs{}
	}
}
