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
			if _, ok := databaseRoles[db]; !ok {
				return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
			}
			dbObjs.grant(ctx, synCnf, cnf, conn, pID, oms[e].DTAP, iID, db, createDBRoleGrants, databaseRoles)
		}
	}
}
