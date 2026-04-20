package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type AggObjAttr struct {
	ObjectType ObjType
	Owner      semantics.Ident

	// set when grant() is called on AggDBObjs
	isSelectGrantedToReadDBRole     bool
	isReferencesGrantedToReadDBRole bool
	isOwnedByProductWriteRole       bool
}

func (o AggObjAttr) setGrantTo(m Mode, g Grant) AggObjAttr {
	switch m {
	case ModeRead:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			o.isSelectGrantedToReadDBRole = true
		case PrvReferences:
			o.isReferencesGrantedToReadDBRole = true
		}
		// Ignore; unmanaged grant
	case ModeWrite:
		switch g.Privileges[0].Privilege {
		case PrvOwnership:
			o.isOwnedByProductWriteRole = true
		}
		// Ignore; unmanaged grant
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
			return o.isSelectGrantedToReadDBRole
		case PrvReferences:
			return o.isReferencesGrantedToReadDBRole
		}
	}
	return false
}

func (o AggObjAttr) pushToDoGrants(yield func(Grant) bool, dbRole DatabaseRole, schema semantics.Ident, obj semantics.Ident) bool {
	prvs := []PrivilegeComplete{}
	// WIP TODO PrvReferences is not there for dynamic tables and online feature tables, are we 100% sure the latter is
	// not in the output of show objects? If so, granting REFERENCES may fail
	for _, p := range [2]Privilege{PrvSelect, PrvReferences} {
		if !o.hasGrantTo(ModeRead, p) {
			prvs = append(prvs, PrivilegeComplete{Privilege: p})
		}
	}
	if len(prvs) > 0 {
		if !yield(Grant{
			Privileges:        prvs,
			GrantedOn:         o.ObjectType,
			Database:          dbRole.Database,
			Schema:            schema,
			Object:            obj,
			GrantedTo:         ObjTpDatabaseRole,
			GrantedToDatabase: dbRole.Database,
			GrantedToName:     dbRole.Name,
		}) {
			return false
		}
	}
	return true
}
