package snowflake

import (
	"fmt"
	"iter"
)

type matchedDBObjs struct {
	version      int
	schemas      map[string]*matchedSchemaObjs
	schemaExists map[string]bool
}

func (o *matchedDBObjs) addSchema(k string) {
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

func (o *matchedDBObjs) hasSchema(k string) bool {
	return o.schemaExists[k]
}

func (o *matchedDBObjs) getSchemas() iter.Seq2[string, *matchedSchemaObjs] {
	return func(yield func(string, *matchedSchemaObjs) bool) {
		for k, v := range o.schemas {
			if o.hasSchema(k) {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}
