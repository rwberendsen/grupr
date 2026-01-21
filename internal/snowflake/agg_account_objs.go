package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)


// AccountObjs aggregated to (product, dtap, interface) level, with fields to store granted privileges on them 
type AggAccountObjs struct {
	DBs map[string]AggDBObjs
}

func newAggAccountObjs(agg AccountObjs) AggAccountObjs {
	r := AggAccountObjs{DBs: map[string]AggDBObjs{},}
	for db, dbObjs := range agg {
		r.DBs[db] = newAggDBObjs(dbObjs)	
	}
	return r
}
