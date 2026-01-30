package snowflake

import (
	"context"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	ObjectMatchers semantics.ObjMatchers
	ConsumedBy map[semantics.ProductDTAPID]struct{}

	// Granular accountObjects by ObjExpr; will be discarded after aggregate() is called
	accountObjects map[semantics.ObjExpr]AccountObjs

	// Computed by aggregate()
	tableCountsByUserGroup map[string]int
	viewCountsByUserGroup map[string]int

	// Computed by aggregate()
	aggAccountObjects AggAccountObjs
}

func NewInterface(dtap string, iSem semantics.InterfaceMetadata) *Interface {
	i := &Interface{
		ObjectMatchers: semantics.ObjMatchers{},
	}	
	// Just take what you need from own DTAP
	for e, om := range iSem.ObjectMatchers {
		if e.DTAP == dtap {
			i.ObjectMatchers[e] = om
		}
	}
	if iSem.ConsumedBy != nil {
		// this is an interface (not a product-level one)
		i.ConsumedBy = map[semantics.ProductDTAPID]struct{}{}
		for dtapSem, pdID := range iSem.ConsumedBy {
			if dtapSem == dtap {
				i.ConsumedBy[pdID] = struct{}{}
			}
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
	for _, om := range i.ObjectMatchers {
		i.accountObjects[e] = newAccountObjects(m[om.SubsetOf], om)
	}
}

func (i *Interface) aggregate() {
	i.setCountsByUserGroup()
	i.setAggAccountObjects()
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
	i.accountObjects = nil // reset, we do not need it anymore, and maps referenced inside this data structure may have been altered while summing
}

func (i *Interface) setFutureGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, createDBRoleGrants map[string]struct{},
		pID string, dtap string, iID string, c *accountCache) error {
	for db, dbObjs := range i.aggAccountObjects.DBs {
		if !c.hasDB(db) {
			return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
		}
		dbObjs, err := dbObjs.setFutureGrants(ctx, synCnf, cnf, conn, pID, dtap, iID, db, i.ObjectMatchers, createDBRoleGrants, c.dbs[db].dbRoles)
		if err != nil { return err }
		i.aggAccountObjects.DBs[db] = dbObjs
	}
}

func (i *Interface) setGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for db, dbObjs := range i.aggAccountObjects.DBs {
		if !c.hasDB(db) {
			return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
		}
		dbObjs, err := dbObjs.setGrants(ctx, synCnf, cnf, conn, db, i.ObjectMatchers)
		if err != nil { return err }
		i.aggAccountObjects.DBs[db] = dbObjs
	}
}

func (i *Interface) pushToDoFutureGrants(yield func(FutureGrant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoFutureGrants(yield) {
			return false
		}
	}
	return true
}

func (i *Interface) pushToDoGrants(yield func(Grant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoGrants(yield) {
			return false
		}
	}
	return true
}

func (i *Interface) pushToDoDBRoleGrants(yield func(Grant) bool, doProd bool, m map[semantics.ProductDTAPID]*ProductDTAP) bool) bool {
	for db, dbObjs := range i.aggAccountObjects.DBs {
		for pdID := range i.ConsumedBy {
			if doProd == m[pdID].IsProd {
				if !dbObjs.consumedByGranted[pd] {
					if !yield(Grant{
						Privilege: PrvUsage,
						GrantedOn: ObjTpDatabaseRole,
						Database: db,
						GrantedRole: dbObjs.dbRole,
						GrantedTo: ObjTpRole,
						GrantedToRole: m[pdID].ReadRole.ID,
					}) {
						return false
					}
				}
			}
		}
	}
	return true
}

func (i *Interface) pushToDoFutureRevokes(yield func(FutureGrant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoFutureRevokes(yield) {
			return false
		}
	}
	return true
}

func (i *Interface) pushToDoRevokes(yield func(Grant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoRevokes(yield) {
			return false
		}
	}
	return true
}
