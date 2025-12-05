package snowflake

import (
	"context"
	"database/sql"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type Matched map[semantics.ObjExpr]*AccountObjs


func newMatched(oms semantics.ObjMatchers) Matched {
	m := Matched{}
	for k, _ := range oms {
		m[k] = &AccountObjs{}
	}
}

func (m *Matched) refresh(ctx context.Context, conn *sql.DB, oms semantics.ObjMatchers, c *accountCache) error {
	for k, v := range m {
		if err := c.match(ctx, conn, k, v); err != nil { return err }
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
	r := Matched{}
	for k := range m.Include {
		r[k] = match(k, r)
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

func matchPart(e semantics.ExprPart, name string) bool {
	if e.IsQuoted {
		return e.S == name
	}
	// implement match unquoted with optional suffix wildcard
	// note that we match case insensitive, so `mytable` would match all of
	// "mytable", "MyTable", "MYTABLE", etc.
	re := semantics.CreateRegexpIdentifier(e.S)
	return re.MatchString(name)
}
