package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	AccountObjects map[semantics.ObjExpr]*AccountObjs
}

func newInterface(m map[semantics.ObjExpr]*AccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		i.AccountObjects[e] = newAccountObjects(m[om.SubsetOf], om)
	}
	return i
}

func newInterfaceFromMatched(m map[semantics.ObjExpr]*matchedAccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		tmpAccountObjs = newAccountObjsFromMatched(m[e])
		i.AccountObjects[e] = newAccountObjects(tmpAccountObjs, om)
	}
	return i
}

func (i Interface) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, createDBRoleGrants map[string]struct{},
		databaseRoles map[string]map[DatabaseRole]struct{}, dtaps semantics.DTAPSpec, pID string, iID string, oms semantics.ObjMatchers) error {
	for e, accObjs := range i.AccountObjects {
		for db, dbObjs := range accObjs.DBs {
			dbRole := newDatabaseRole(synCnf, conf, pID, oms[e].DTAP, iID, ModeRead, db)
			if _, ok := databaseRoles[db]; !ok {
				return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
			}
			if _, ok := databaseRoles[db][dbRole]; !ok {
				if _, ok = createDBRoleGrants[db]; !ok {
					if err := GrantCreateDatabaseRoleToSelf(ctx, cnf, conn, db); err != nil { return err }
				}
				if err := dbRole.Create(ctx, cnf, conn); err != nil { return err }
			} else {
				dbObjs.queryGrants(ctx, conn, dbRole)
			}
			dbObjs.grant(ctx, cnf, conn)
		}
			// SHOW GRANTS TO / ON / OF database role, and store them in DBObjs
			// grants on objects should be stored on the respective accountobjects
			// if no accountobjects is there, it means this is a grant that should be revoked, later,
			// and it should be stored separately, for later processing, after all grants have been done.
	}
}
