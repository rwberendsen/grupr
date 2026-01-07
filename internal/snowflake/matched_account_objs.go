package snowflake

import (
	"fmt"
)

type matchedAccountObjs struct {
	version int
	dbs map[string]*matchedDBObjs
	dbExists map[string]bool
}

func (o *matchedAccountObjs) addDB(k string) {
	if o.dbs == nil {
		o.dbs = map[string]*matchedDBObjs{}
		o.dbExists = map[string]bool{}
	}
	if _, ok := o.dbs[k]; !ok {
		o.dbs[k] = &matchedDBObjs{}
	}
	o.dbExists[k] = true
}

func (o *matchedAccountObjs) dropDB(k string) {
	if _, ok := o.dbs[k]; !ok {
		panic(fmt.Sprintf("string not found: '%s'", k))
	}
	o.dbExists[k] = false
}

func (o *matchedAccountObjs) hasDB(k string) bool {
	return o.dbExists != nil && o.dbExists[k]
}

func (o *matchedAccountObjs) getDBs() iter.Seq2[string, *matchedDBObjs] {
	return func(yield func(string, *matchedDBObjs) bool) {
		for k, v := range o.dbs {
			if o.dbExists[k] {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}
