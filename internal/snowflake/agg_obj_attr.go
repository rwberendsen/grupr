package snowflake

import (
	"context"
	"database/sql"
)

type AggObjAttr struct {
	ObjectType 	ObjType
	Owner 		string
	
	// set when grant() is called on AggDBObjs
	isSelectGrantedToRead bool
	isReferencesGrantedToRead bool
}

func (o AggObjAttr) setGrantTo(m Mode, p Privilege) AggObjAttr {
	switch m {
	case ModeRead:
		switch p {
		case PrvSelect:
			o.isSelectGrantedToRead = true
		case PrvReferences:
			o.isReferencesGrantedToRead = true
		default:
			panic("not implemented")
		}
	default:
		panic("not implemented")
	}
	return o
}

func (o AggObjAttr) hasGrantTo(m Mode, p Privilege) bool {
	switch m {
	case ModeRead:
		switch p {
		case PrvSelect:
			return o.isSelectGrantedToRead
		case PrvReferences:
			return o.isReferencesGrantedToRead
		}
	}
	return false
}

func (o AggObjAttr) pushToDoGrants(yield func(Grant) bool, dbRole DatabaseRole, schema string, obj string) bool {
	for _, p := range [2]Privilege{PrvSelect, PrvReferences} {
		// TODO: combine select and references in a single grant statement
		if !o.hasGrantTo(ModeRead, p) {
			if !yield(Grant{
				Privilege: p,
				GrantedOn: ObjTpSchema,
				Database: dbRole.Database,
				Schema: schema,
				Object: obj,
				GrantedTo: ObjTpDatabaseRole,
				GrantedToDatabase: dbRole.Database,
				GrantedToRole: o.dbRole.Name,
			}) {
				return false
			}
		}
	}
	return true
}
