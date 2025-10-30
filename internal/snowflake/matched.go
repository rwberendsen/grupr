package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Matched struct {
	Objects map[semantics.ObjExpr]AccountObjs
}

func newMatched(m semantics.ObjMatcher, c *accountCache) Matched {
	r := Matched{map[semantics.ObjExpr]AccountObjs{}}
	for k := range m.Include {
		r.Objects[k] = match(k, c)
	}
	for k := range m.Exclude {
		r.Objects[m.StrictSuperset[k]] = r.Objects[m.StrictSuperset[k]].subtract(match(k, c))
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

func match(e semantics.ObjExpr, c *accountCache) AccountObjs {
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
