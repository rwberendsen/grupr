package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type AggSchemaObjs struct {
	Objects         map[semantics.Ident]AggObjAttr
	MatchAllObjects bool

	// set while grants are being set
	isUsageGrantedToRead             bool
	isPrivilegeGrantedToFutureObject [2][2]bool // [ObjType][Privilege]
	isCreateGrantedToWrite           [2]bool    // [ObjType]
}

func newAggSchemaObjs(o SchemaObjs) AggSchemaObjs {
	r := AggSchemaObjs{
		Objects:         make(map[semantics.Ident]AggObjAttr, len(o.Objects)),
		MatchAllObjects: o.MatchAllObjects,
	}
	for k, v := range o.Objects {
		r.Objects[k] = AggObjAttr{ObjectType: v.ObjectType}
	}
	return r
}

func (o AggSchemaObjs) hasObject(k semantics.Ident) bool {
	_, ok := o.Objects[k]
	return ok
}

func (o AggSchemaObjs) setFutureGrantTo(m Mode, g FutureGrant) AggSchemaObjs {
	switch g.GrantedOn {
	case ObjTpTable, ObjTpView:
		switch g.Privileges[0].Privilege {
		case PrvSelect, PrvReferences:
			o.isPrivilegeGrantedToFutureObject[g.GrantedOn.getIdxObjectLevel()][g.Privileges[0].Privilege.getIdxObjectLevel()] = true
		}
		// Ignore; unmanaged grant
	}
	// Ignore; unmanaged grant
	return o
}

func (o AggSchemaObjs) hasFutureGrantTo(m Mode, grantedOn ObjType, p Privilege) bool {
	switch grantedOn {
	case ObjTpTable, ObjTpView:
		switch p {
		case PrvSelect, PrvReferences:
			return o.isPrivilegeGrantedToFutureObject[grantedOn.getIdxObjectLevel()][p.getIdxObjectLevel()]
		}
	}
	return false
}

func (o AggSchemaObjs) setGrantTo(m Mode, g Grant) AggSchemaObjs {
	if m == ModeRead && g.Privileges[0].Privilege == PrvUsage {
		o.isUsageGrantedToRead = true
	}
	if m == ModeWrite && g.Privileges[0].Privilege == PrvCreate &&
	(g.Privileges[0].CreateObjectType == ObjTpTable || g.Privileges[0].CreateObjectType == ObjTpView) {
		o.isCreateGrantedToWrite[g.Privileges[0].CreateObjectType.getIdxObjectLevel()] = true
	}
	// Ignore; unmanaged grant
	return o
}

func (o AggSchemaObjs) hasGrantTo(m Mode, p Privilege) bool {
	return m == ModeRead && p == PrvUsage && o.isUsageGrantedToRead
}

func (o AggSchemaObjs) pushToDoFutureGrants(yield func(FutureGrant) bool, dbRole DatabaseRole, schema semantics.Ident) bool {
	if o.MatchAllObjects {
		for _, ot := range [2]ObjType{ObjTpTable, ObjTpView} {
			prvs := []PrivilegeComplete{}
			for _, p := range [2]Privilege{PrvSelect, PrvReferences} {
				if !o.hasFutureGrantTo(ModeRead, ot, p) {
					prvs = append(prvs, PrivilegeComplete{Privilege: p})
				}
			}
			if len(prvs) > 0 {
				if !yield(FutureGrant{
					Privileges:        prvs,
					GrantedOn:         ot,
					GrantedIn:         ObjTpSchema,
					Database:          dbRole.Database,
					Schema:            schema,
					GrantedTo:         ObjTpDatabaseRole,
					GrantedToDatabase: dbRole.Database,
					GrantedToName:     dbRole.Name,
				}) {
					return false
				}
			}
		}
	}
	return true
}

func (o AggSchemaObjs) pushToDoGrants(yield func(Grant) bool, dbRole DatabaseRole, schema semantics.Ident) bool {
	if !o.hasGrantTo(ModeRead, PrvUsage) {
		if !yield(Grant{
			Privileges:        []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
			GrantedOn:         ObjTpSchema,
			Database:          dbRole.Database,
			Schema:            schema,
			GrantedTo:         ObjTpDatabaseRole,
			GrantedToDatabase: dbRole.Database,
			GrantedToName:     dbRole.Name,
		}) {
			return false
		}
	}
	for obj, objAttr := range o.Objects {
		if !objAttr.pushToDoGrants(yield, dbRole, schema, obj) {
			return false
		}
	}
	return true
}
