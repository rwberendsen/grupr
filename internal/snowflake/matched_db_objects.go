package snowflake

import (
	"fmt"
	"iter"
)

type struct matchedDBObjs {
	version int
	schemas map[string]*matchedSchemaObjs
	schemaExists map[string]bool
}

func (o *matchedObjs) addSchema(k string) {
	if o.schemas == nil {
		o.schemas = map[string]*matchedSchemaObjs{}
		o.schemaExists = map[string]bool{}
	}
	if _, ok := o.schemas[k]; !ok {
		o.schemas[k] = &matchedSchemaObjs{}
	}
	o.schemaExists[k] = true
}

func (o *matchedDBObjs) dropSchema(k string) {
	if _, ok := o.schemas[k]; !ok {
		panic(fmt.Sprintf("Schema not found: '%s'", k))
	}
	o.schemaExists[k] = false
}

func (o *matchedDBObjs) hasSchema(k string) {
	return o.schemaExists != nil && o.schemaExists[k]
}

func (o *matchedDBObjs) getSchemas() iter.Seq2[string, *matchedSchemaObjs] {
	return func(yield func(string, *matchedSchemaObjs) bool) {
		for k, v := range o.schemas {
			if o.schemaExists(k) {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}
