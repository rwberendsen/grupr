package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Matched struct {
	Objects map[semantics.ObjExpr]AccountObjs
}

func newMatchedAgainstAccountCache(m semantics.ObjMatcher, c *accountCache) Matched {
	r := Matched{map[semantics.ObjExpr]AccountObjs{}}
	for k := range m.Include {
		r.Objects[k] = matchAgainstAccountCache(k, c)
	}
	for k := range m.Exclude {
		// TODO: match against r.Objects, cause other threads may have manipulated the accountcache concurrently,
		// e.g., if excluded objects were deleted concurrently, we would not subtract them anymore
		r.Objects[m.StrictSuperset[k]] = r.Objects[m.StrictSuperset[k]].subtract(match(k, c))
		// TODO: also, when we are matching an interface, we might decide to match against superset defined in
		// product, cause it may be faster (fewer objects to match against)
	}
	return r
}

func newMatchedAgainstAccountObjs(m semantics.ObjMatcher, o AccountObjs) Matched {
	r := Matched{map[semantics.ObjExpr]AccountObjs{}}
	for k := range m.Include {
		r.Objects[k] = match(k, r)
	}
	for k := range m.Exclude {
		// TODO: match against r.Objects, cause other threads may have manipulated the accountcache concurrently,
		// e.g., if excluded objects were deleted concurrently, we would not subtract them anymore
		r.Objects[m.StrictSuperset[k]] = r.Objects[m.StrictSuperset[k]].subtract(match(k, c))
		// TODO: also, when we are matching an interface, we might decide to match against superset defined in
		// product, cause it may be faster (fewer objects to match against)
	}
	return r
}

func matchPart(e semantics.ExprPart, l map[string]bool) map[string]bool {
	r := map[string]bool{}
	if e.IsQuoted {
		if _, ok := l[e.S]; ok {
			r[e.S] = true
		}
		return r
	}
	// implement match unquoted with optional suffix wildcard
	// note that we match case insensitive, so `mytable` would match all of
	// "mytable", "MyTable", "MYTABLE", etc.
	re := semantics.CreateRegexpIdentifier(e.S)
	for k := range l {
		if re.MatchString(k) {
			r[k] = true
		}
	}
	return r
}

func matchAgainstAccountCache(e semantics.ObjExpr, c *accountCache) AccountObjs {
	// we might decide to handle errors in a function like this, e.g., if we matched a schema, but, before we list objects in it, it was dropped, and Snowflake returns an error that the schema does not
        // exist. In that case, we might back-track, and list schemas again, and if that gives an error because the database was dropped, we might back-track and list DB's again. If Snowflake keeps throwing errors,
        // either objects are still being dropped, or perhaps we are lacking some access. What to do in such cases? Perhaps we should propagate errors further back up the chain instead?
	o := AccountObjs{}
	dbs, accountVersion := c.getDBs()
	matchedDBs := matchPart(e[semantics.Database], dbNames)
	o.Version = accountVersion
	for db := range matchedDBs {
		schemas, dbVersion := dbs[db].getSchemas(c)
		matchedSchemas := matchPart(e[semantics.Schema], schemaNames)
		o = o.addDB(db, e[semantics.Schema].MatchAll(), dbVersion)
		for schema := range matchedSchemas {
			objectNames, schemaVersion := schemas[schema].getObjectNames(c)
			// it is interesting to consider the case where the schema we are trying to list objects in has been removed concurrently. In fact, in this case, another thread may have beaten us to
			// it and it may have also removed this schema from the account cache already. So our schemas[schema] reference is still valid for us, but it can't be reached anymore via the accountcache.
			// in any case, then schemas[schema].getObjectNames(c) will experience an error. Probably I'd want to catch that here. And probably break out of the loop as well, and try and list schemas
			// again. Or, just give it back to the caller, the error, so the caller can "just" try again to call us
			//
			// Another approach could be to "just" ignore all such errors: we would say: yes the account is fluid, objects come and go. If they are there, we will find them. If they are not there,
			// and we try to grant privileges on them, but we get an error because they don't exist, no harm done.
			matchedObjects := matchPart(e[semantics.Table], objectNames)
			o = o.addSchema(db, schema, e[semantics.Table].MatchAll(), schemaVersion)
			for t := range matchedTables {
				o = o.addTable(db, schema, t)
			}
			for v := range matchedViews {
				o = o.addView(db, schema, v)
			}
		}
	}
	return o
}
