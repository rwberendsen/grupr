package snowflake

import (
	"context"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	ObjectMatchers semantics.ObjMatchers
	accountObjects map[semantics.ObjExpr]AccountObjs

	// set during calculating objects
	tableCountsByUserGroup map[string]int
	viewCountsByUserGroup map[string]int

	aggAccountObjects AggAccountObjs
}

func NewInterface(dtap string, oms semantics.ObjMatchers) Interface {
	i := &Interface{
		ObjectMatchers: semantics.ObjMatchers{},
		accountObjects: map[semantics.ObjExpr]AccountObjs{},
	}	
	// Just take what you need from own DTAP
	for e, om := range oms {
		if e.DTAP == dtap {
			i.ObjectMatchers[e] = om
		}
	}
	return i
}

func newInterfaceFromMatched(m map[semantics.ObjExpr]*matchedAccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{accountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		tmpAccountObjs = newAccountObjsFromMatched(m[e])
		i.accountObjects[e] = newAccountObjects(tmpAccountObjs, om)
	}
	return i
}

func (i Interface) refresh(m map[semantics.ObjExpr]*accountObjects, oms semantics.ObjMatchers) Interface {
	for e, om := range oms {
		i.accountObjects[e] = newAccountObjects(m[om.SubsetOf], om)
	}
	return i
}

func (i Interface) setAggAccountObjects() {
	i.aggAccountObjects = newAggAccountObjs(maps.Values(i.accountObjects))
}

func (i Interface) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, createDBRoleGrants map[string]struct{},
		dtaps semantics.DTAPSpec, pID string, iID string, oms semantics.ObjMatchers, c *accountCache) error {
	i.aggAccountObjects = newAggAccountObjs(maps.Values(i.accountObjects))
	i.accountObjects = nil // no need to retain separate accountObjects per ObjExpr in memory anymore
	for db, dbObjs := range agg.DBs {
		if !c.hasDB(db) {
			return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
		}
		dbObjs.grant(ctx, synCnf, cnf, conn, pID, oms[e].DTAP, iID, db, createDBRoleGrants, c.dbs[db].dbRoles)
	}
}
