package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	ObjectMatchers semantics.ObjMatchers
	AccountObjects map[semantics.ObjExpr]*AccountObjs
}

func NewInterface(dtap string, oms semantics.ObjMatchers) Interface {
	i := &Interface{
		ObjectMatchers: semantics.ObjMatchers{},
		AccountObjects: map[semantics.ObjExpr]*AccountObjs{},
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
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		tmpAccountObjs = newAccountObjsFromMatched(m[e])
		i.AccountObjects[e] = newAccountObjects(tmpAccountObjs, om)
	}
	return i
}

func (i Interface) refresh(m map[semantics.ObjExpr]*AccountObjects, oms semantics.ObjMatchers) Interface {
	for e, om := range oms {
		i.AccountObjects[e] = newAccountObjects(m[om.SubsetOf], om)
	}
	return i
}

// WIP
func newInterfaceFromMatched(m map[semantics.ObjExpr]*matchedAccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		tmpAccountObjs = newAccountObjsFromMatched(m[e])
		i.AccountObjects[e] = newAccountObjects(tmpAccountObjs, om)
	}
	return i
}

func (i Interface) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, createDBRoleGrants map[string]struct{},
		dtaps semantics.DTAPSpec, pID string, iID string, oms semantics.ObjMatchers, c *accountCache) error {
	for e, accObjs := range i.AccountObjects {
		for db, dbObjs := range accObjs.DBs {
			if !c.hasDB(db) {
				return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
			}
			dbObjs.grant(ctx, synCnf, cnf, conn, pID, oms[e].DTAP, iID, db, createDBRoleGrants, c.dbs[db].dbRoles)
		}
	}
}
