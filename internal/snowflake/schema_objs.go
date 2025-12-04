package snowflake

type SchemaObjs struct {
	Version int // version with regard to schemaCache
	Objects map[objKey]struct
	MatchAllObjects bool
}

func (o *SchemaObjs) addObject(k objKey) {
	if _, ok := o.Objects[k]; !ok {
		if o.Objects == nil {
			o.Objects = map[objKey]struct{}
		}
		o.Objects[k] = struct{}
	}
}
