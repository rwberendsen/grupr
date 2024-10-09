package snowflake

import (
	"log"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type Matched struct {
	Objects map[semantics.Expr]AccountObjs
}

func newMatched(m semantics.Matcher, c *accountCache) Matched {
	r := Matched{map[semantics.Expr]AccountObjs{}}
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

func match(e semantics.Expr, c *accountCache) AccountObjs {
	o := AccountObjs{}
	matchedDBs := matchPart(e[semantics.Database], c.getDBnames())
	log.Printf("DBnames: %v", c.getDBnames())
	log.Printf("matchedDBS: %v", matchedDBs)
	for db := range matchedDBs {
		o = o.addDB(db, e[semantics.Schema].MatchAll())
		matchedSchemas := matchPart(e[semantics.Schema], c.getDBs()[db].getSchemaNames())
		for schema := range matchedSchemas {
			o = o.addSchema(db, schema, e[semantics.Table].MatchAll())
			matchedTables := matchPart(e[semantics.Table], c.getDBs()[db].getSchemas()[schema].getTableNames())
			matchedViews := matchPart(e[semantics.Table], c.getDBs()[db].getSchemas()[schema].getViewNames())
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
