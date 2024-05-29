package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Matched struct {
	Objects accountObjs
	include accountObjs
	exclude accountObjs
}

func newMatched(m semantics.Matcher, c *accountCache) Matched {
	r := Matched{}
	for e, _ := range m.Include {
		r.include = r.include.add(match(e, c))
	}
	for e, _ := range m.Exclude {
		r.exclude = r.Exclude.add(match(e, c))
	}
	r.Objects = r.include.subtract(r.exclude)
	return r
}

func matchPart(e semantics.ExprPart, l map[string]bool) map[string]bool {
	r := map[string]bool{}
	if e.Is_quoted {
		if _, ok := l[e.S]; ok {
			r[e.S] = true
		}
		return r
	}
	// implement match unquoted with optional suffix wildcard
	// note that we match case insensitive, so `mytable` would match all of
	// "mytable", "MyTable", "MYTABLE", etc.
	re := semantics.CreateRegexpIdentifier(e.S)
	for k, _ := range l {
		if re.MatchString(k) {
			r[k] = true
		}
	}
	return r
}

func match(e semantics.Expr, c *accountCache) accountObjs {
	o := accountObjs{}
	matchedDBs := matchPart(e[semantics.Database], c.getDBnames())
	for db, _ := range matchedDBs {
		o = o.addDB(db, e[semantics.Schema].MatchAll())
		matchedSchemas := matchPart(e[semantics.Schema], c.getDBs()[db].getSchemaNames())
		for schema, _ := range matchedSchemas {
			o = o.addSchema(db, schema, e[semantics.Table].MatchAll())
			matchedTables := matchPart(e[semantics.Table], c.getDBs()[db].getSchemas()[schema].getTableNames())
			matchedViews := matchPart(e[semantics.Table], c.getDBs()[db].getSchemas()[schema].getViewNames())
			for t, _ := range matchedTables {
				o = o.addTable(db, schema, t)
			}
			for v, _ := range matchedViews {
				o = o.addView(db, schema, v)
			}
		}
	}
	return o
}