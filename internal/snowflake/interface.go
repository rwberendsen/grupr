package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	AccountObjects map[semantics.ObjExpr]*AccountObjs
}

func newInterfaceFromMatched(m map[semantics.ObjExpr]*matchedAccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		tmpAccountObjs = newAccountObjsFromMatched(m[e])
		i.AccountObjects[e] = newAccountObjects(tmpAccountObjs, e, om)
	}
	return i
}

func newInterface(m map[semantics.ObjExpr]*AccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		i.AccountObjects[e] = newAccountObjects(m[om.SubsetOf])
	}
	return i
}

func (i *Interface) grant(ctx context.Context, cnf *Config, conn *sql.DB, databaseRoles map[string]bool) error {
	for db, dbObjs := range i.AccountObjs {
		if databaseRoles[db]['prefix_pid_r'] {
			// SHOW GRANTS TO / ON / OF database role, and store them in DBObjs
		} else {
			// CREATE DATABASE ROLE, and store empty grants in DBObjs
			if cnf.DryRUn { // then don't execute command, but merely log it
			}
		}
		// compute necessary grant statements
		// GRANT privilege to ROLE ... if dry run then merely log it
	}
}
