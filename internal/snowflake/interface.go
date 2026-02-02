package snowflake

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	ObjectMatchers semantics.ObjMatchers
	UserGroups syntax.Rendering
	ConsumedBy map[semantics.ProductDTAPID]struct{}

	// Granular accountObjects by ObjExpr; will be discarded after aggregate() is called
	accountObjects map[semantics.ObjExpr]AccountObjs

	// Computed by aggregate()
	objectCountsByUserGroup map[string]map[ObjType]int

	// Computed by aggregate()
	aggAccountObjects AggAccountObjs
	
	// For use in pushObjectCounts
	userGroupsStr string
}

func NewInterface(dtap string, iSem semantics.InterfaceMetadata) *Interface {
	i := &Interface{
		ObjectMatchers: semantics.ObjMatchers{},
		UserGroups: iSem.UserGroups,
		userGroupsStr: strings.Join(slices.Sorted(maps.Keys(iSem.UserGroups)), ",")
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
	i.objectCountsByUserGroup = map[string]map[ObjType]int
	for e, om := range i.ObjectMatchers {
		if i.objectCountsByUserGroup[om.UserGroup] = nil {
			i.objectCountsByUserGroup[om.UserGroup] = map[ObjType]int{}
		}
		i.objectCountsByUserGroup[om.UserGroup][ObjTpTable] += i.accountObjects[e].countByObjType(ObjTpTable)
		i.objectCountsByUserGroup[om.UserGroup][ObjTpView] += i.accountObjects[e].countByObjType(ObjTpView)
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
						Privileges: []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage,}},
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

func (i *Interface) pushObjectCounts(yield func(ObjCountsRow) bool, pdID semantics.ProductDTAPID, iid string) bool {
	for ug, countsByObjType := range i.objectCountsByUserGroup {
		r := ObjCountsRow{
			ProductID: pdID.ProductID,
			DTAP: pdID.DTAP,
			InterfaceID: iid,
			UserGroups: ug,
			TableCount: countsByObjType[ObjTpTable],
			ViewCount: countsByObjType[ObjTpView],
		}
		if ug == "" {
			r.UserGroups = i.userGroupsStr
		}
		if !yield(r) { return false }
	}
	return true
}
