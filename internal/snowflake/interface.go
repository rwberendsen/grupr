package snowflake

import (
	"context"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	ObjectMatchers semantics.ObjMatchers

	// Granular accountObjects by ObjExpr; will be discarded after aggregate() is called
	accountObjects map[semantics.ObjExpr]AccountObjs

	// Computed by aggregate()
	tableCountsByUserGroup map[string]int
	viewCountsByUserGroup map[string]int

	// Computed by aggregate()
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
}

func (i *Interface) recalcObjects(m map[semantics.ObjExpr]AccountObjs) {
	// Called from the interface level, work with SubsetOf here
	i.accountObjects = map[semantics.ObjExpr]AccountObjs{} // (re)set
	for e, om := range i.ObjectMatchers {
		i.accountObjects[e] = newAccountObjects(m[om.SubsetOf], om)
	}
}

func (i *Interface) aggregate() {
	i.setCountsByUserGroup()
	i.setAggAccountObjects()
	i.accountObjects = nil // reset
}

func (i *Interface) setCountsByUserGroup() {
	i.tableCountsByUserGroup = map[string]int{}
	i.viewCountsByUserGroup = map[string]int{}
	for e, om := range i.ObjectMatchers {
		i.tableCountsByUserGroup[om.UserGroup] += i.accountObjects[e].countByObjType(ObjTpTable)
		i.viewCountsByUserGroup[om.UserGroup] += i.accountObjects[e].countByObjType(ObjTpView)
	}
}

func (i *Interface) setAggAccountObjects() {
	sum := AccountObjs{}
	for _, o := range maps.Values(i.accountObjects) {
		sum.add(o)
	}
	i.aggAccountObjects = newAggAccountObjs(sum)
}

func (i *Interface) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, createDBRoleGrants map[string]struct{},
		pID string, dtap string, iID string, c *accountCache) error {
	for db, dbObjs := range i.aggAccountObjects.DBs {
		if !c.hasDB(db) {
			return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
		}
		dbObjs, err := dbObjs.grant(ctx, synCnf, cnf, conn, pID, dtap, iID, db, createDBRoleGrants, c.dbs[db].dbRoles)
		if err != nil { return err }
		i.aggAccountObjects.DBs[db] = dbObjs
	}
}

func (i *Interface) pushToDoGrants(yield func(Grant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoGrants(yield) {
			return false
		}
	}
	return true
}
