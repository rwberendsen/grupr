package snowflake

import (
	"fmt"
	"iter"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type matchedAccountObjs struct {
	version  int
	dbs      map[semantics.Ident]*matchedDBObjs
	dbExists map[semantics.Ident]bool
}

func (o *matchedAccountObjs) addDB(k semantics.Ident) {
	if o.dbs == nil {
		o.dbs = map[semantics.Ident]*matchedDBObjs{}
		o.dbExists = map[semantics.Ident]bool{}
	}
	if _, ok := o.dbs[k]; !ok {
		o.dbs[k] = &matchedDBObjs{}
	}
	o.dbExists[k] = true
}

func (o *matchedAccountObjs) dropDB(k semantics.Ident) {
	if _, ok := o.dbs[k]; !ok {
		panic(fmt.Sprintf("Ident not found: '%s'", k))
	}
	o.dbExists[k] = false
}

func (o *matchedAccountObjs) hasDB(k semantics.Ident) bool {
	return o.dbExists != nil && o.dbExists[k]
}

func (o *matchedAccountObjs) getDBs() iter.Seq2[semantics.Ident, *matchedDBObjs] {
	return func(yield func(semantics.Ident, *matchedDBObjs) bool) {
		for k, v := range o.dbs {
			if o.dbExists[k] {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}
