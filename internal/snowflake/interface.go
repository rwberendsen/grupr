package snowflake

import (
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

func (i Interface) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, databaseRoles map[string]map[DatabaseRole]struct{},
		dtaps semantics.DTAPSpec, pID string, iID string, oms semantics.ObjMatchers) error {
	for e, accObjs := range i.AccountObjects {
		for db, dbObjs := range accObjs.DBs {
			for mode := range cnf.Modes {
				dbRole := newDatabaseRole(synCnf, conf, pID, oms[e].DTAP, iID, mode, db)
				if _, ok := databaseRoles[db]; !ok {
					return ErrObjectNotExistOrAuthorized // db no longer there in cache, indicating removal of DB
				}
				if _, ok := databaseRoles[db][dbRole]; !ok {
					if err := dbRole.create(ctx, cnf, conn, len(databaseRoles[db]) == 0); err != nil { return err }
				}
			}
		}
		oms[e].DTAP
		if databaseRoles[db]['prefix_pid_[iid]_r'] {
			// SHOW GRANTS TO / ON / OF database role, and store them in DBObjs
			// grants on objects should be stored on the respective accountobjects
			// if no accountobjects is there, it means this is a grant that should be revoked, later,
			// and it should be stored separately, for later processing, after all grants have been done.
		} else {
			// CREATE DATABASE ROLE, and store empty grants in DBObjs
			if cnf.DryRUn { // then don't execute command, but merely print it
			}
		}
		// compute necessary grant statements
		// GRANT privilege to ROLE ... if dry run then merely log it
		// loop over accountobjs to 
	}
}
