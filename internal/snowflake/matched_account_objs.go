package snowflake

import (
	"fmt"
)

type matchedAccountObjs struct {
	version int
	dbs map[DBKey]*matchedDBObjs
	dbExists map[DBKey]bool
}

func (o *matchedAccountObjs) addDB(k DBKey) {
	if o.dbs == nil {
		o.dbs = map[DBKey]*matchedDBObjs{}
		o.dbExists = map[DBKey]bool{}
	}
	if _, ok := o.dbs[k]; !ok {
		o.dbs[k] = &matchedDBObjs{}
	}
	o.dbExists[k] = true
}

func (o *matchedAccountObjs) dropDB(k DBKey) {
	if _, ok := o.dbs[k]; !ok {
		panic(fmt.Sprintf("DBKey not found: '%s'", k))
	}
	o.dbExists[k] = false
}

func (o *matchedAccountObjs) hasDB(k DBKey) {
	return o.dbExists[k]
}
