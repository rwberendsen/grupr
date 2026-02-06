package snowflake

// AccountObjs aggregated to (product, dtap, interface) level, with fields to store granted privileges on them
type AggAccountObjs struct {
	DBs map[string]AggDBObjs
}

func newAggAccountObjs(o AccountObjs) AggAccountObjs {
	r := AggAccountObjs{DBs: make(map[string]AggDBObjs, len(o.DBs))}
	for db, dbObjs := range o.DBs {
		r.DBs[db] = newAggDBObjs(dbObjs)
	}
	return r
}
