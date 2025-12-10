package snowflake

import (
	"fmt"
)

type struct matchedDBObjs {
	version int
	schemas map[string]*matchedDBObjs
	schemaExists map[string]bool
}

func (o *matchedDBObjs) addSchema(k string) {
	if o.schemas == nil {
		o.schemas = map[string]*matchedDBObjs{}
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
