package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type AggObjAttr struct {
	ObjectType ObjType
	Owner      semantics.Ident

	// set when grant() is called on AggDBObjs
	isSelectGrantedToReadDBRole           bool
	isSelectErrorTableGrantedToReadDBRole bool
	isReferencesGrantedToReadDBRole       bool
	isOwnedByProductWriteRole             bool
}

func (o AggObjAttr) setGrantTo(m Mode, g Grant) AggObjAttr {
	switch m {
	case ModeRead:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			o.isSelectGrantedToReadDBRole = true
		case PrvSelectErrorTable:
			// Note, this is of course only valid for a regular table,
			// but, we won't expect it to be called with an invalid grant like doing this on a view, for example.
			o.isSelectErrorTableGrantedToReadDBRole = true
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
		case PrvSelectErrorTable:
			return o.isSelectErrorTableGrantedToReadDBRole
		case PrvReferences:
			return o.isReferencesGrantedToReadDBRole
		}
	}
	return false
}

func (o AggObjAttr) pushToDoGrants(yield func(Grant) bool, dbRole DatabaseRole, schema semantics.Ident, obj semantics.Ident) bool {
	prvs := []PrivilegeComplete{}
	candidatePrvs := []PrivilegeComplete{
		PrivilegeComplete{Privilege: PrvSelect},
		PrivilegeComplete{Privilege: PrvReferences},
	}
	if o.ObjectType == ObjTpTable {
		candidatePrvs = append(candidatePrvs, PrivilegeComplete{Privilege: PrvSelectErrorTable})
	}
	for _, pc := range candidatePrvs {
		if !o.hasGrantTo(ModeRead, pc.Privilege) {
			prvs = append(prvs, pc)
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
