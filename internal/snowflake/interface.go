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

func NewInterface(dtap string, oms semantics.ObjMatchers) *Interface {
	i := &Interface{
		ObjectMatchers: semantics.ObjMatchers{},
	}	
	// Just take what you need from own DTAP
	for e, om := range oms {
		if e.DTAP == dtap {
			i.ObjectMatchers[e] = om
		}
	}
	return i
}

func (i *Interface) recalcObjectsFromMatched(m map[semantics.ObjExpr]*matchedAccountObjects) {
	// Called from the product level only
	i.accountObjects = map[semantics.ObjExpr]AccountObjs{} // (re)set
	for e, om := range i.ObjectMatchers {
		tmpAccountObjs = newAccountObjsFromMatched(m[e])
		i.accountObjects[e] = newAccountObjects(tmpAccountObjs, om)
	}
	// WIP compute aggregate stuff here
}

func (i *Interface) recalcObjects(m map[semantics.ObjExpr]AccountObjs) {
	// Called from the interface level, work with SubsetOf here
	i.accountObjects = map[semantics.ObjExpr]AccountObjs{} // (re)set
	for e, om := range i.ObjectMatchers {
		i.accountObjects[e] = newAccountObjects(m[om.SubsetOf], om)
	}
	// WIP compute aggregate stuff here
}

func (i *Interface) doAggAccountObjects() {
	sum := AccountObjs{}
	for _, o := range maps.Values(i.accountObjects) {
		sum.add(o)
	}
	i.aggAccountObjects = newAggAccountObjs(sum)
	i.accountObjects = nil // reset
}

func (i *Interface) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, createDBRoleGrants map[string]struct{},
		dtaps semantics.DTAPSpec, pID string, iID string, oms semantics.ObjMatchers, c *accountCache) error {
	i.setAggAccountObjects() = newAggAccountObjs(maps.Values(i.accountObjects))
	for db, dbObjs := range agg.DBs {
		if !c.hasDB(db) {
			return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
		}
		dbObjs.grant(ctx, synCnf, cnf, conn, pID, oms[e].DTAP, iID, db, createDBRoleGrants, c.dbs[db].dbRoles)
	}
}
