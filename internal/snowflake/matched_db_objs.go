package snowflake

import (
	"fmt"
	"iter"
	
	"github.com/rwberendsen/grupr/internal/semantics"
)

type matchedDBObjs struct {
	version      int
	schemas      map[semantics.Ident]*matchedSchemaObjs
	schemaExists map[semantics.Ident]bool
}

func (o *matchedDBObjs) addSchema(k semantics.Ident) {
	if o.schemas == nil {
		o.schemas = map[semantics.Ident]*matchedSchemaObjs{}
		o.schemaExists = map[semantics.Ident]bool{}
	}
	if _, ok := o.schemas[k]; !ok {
		o.schemas[k] = &matchedSchemaObjs{}
	}
	o.schemaExists[k] = true
}

func (o *matchedDBObjs) dropSchema(k semantics.Ident) {
	if _, ok := o.schemas[k]; !ok {
		panic(fmt.Sprintf("Schema not found: '%s'", k))
	}
	o.schemaExists[k] = false
}

func (o *matchedDBObjs) hasSchema(k semantics.Ident) bool {
	return o.schemaExists[k]
}

func (o *matchedDBObjs) getSchemas() iter.Seq2[semantics.Ident, *matchedSchemaObjs] {
	return func(yield func(semantics.Ident, *matchedSchemaObjs) bool) {
		for k, v := range o.schemas {
			if o.hasSchema(k) {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}
