package snowflake

type DBObjs struct {
	Version int // version with regard to dbCache
	Schemas  map[string]*SchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool
}

func (o *DBObjs) addSchema(schema string) {
	if _, ok := o.Schemas[schema]; !ok {
		if o.Schemas == nil {
			o.Schemas = map[string]*SchemaObjs{}
		}
		o.Schemas[schema] = &SchemaObjs{}
	}
}
