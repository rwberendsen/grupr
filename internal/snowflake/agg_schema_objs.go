package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type AggSchemaObjs struct {
	Objects         map[semantics.Ident]AggObjAttr
	MatchAllObjects bool

	// set while grants are being set
	// We need also monitor
	isPrivilegeOnFutureObjectGrantedToReadDBRole [7]bool
	isPrivilegeGrantedToReadDBRole               [2]bool
	isCreateGrantedToProductWriteRole            [3]bool
}

func newAggSchemaObjs(o SchemaObjs) AggSchemaObjs {
	r := AggSchemaObjs{
		Objects:         make(map[semantics.Ident]AggObjAttr, len(o.Objects)),
		MatchAllObjects: o.MatchAllObjects,
	}
	for k, v := range o.Objects {
		r.Objects[k] = AggObjAttr{ObjectType: v.ObjectType, Owner: v.Owner}
	}
	return r
}

func (o AggSchemaObjs) hasObject(k semantics.Ident) bool {
	_, ok := o.Objects[k]
	return ok
}

func (o AggSchemaObjs) setFutureGrantTo(_ Mode, g FutureGrant) AggSchemaObjs {
	// Currently, only ModeRead privileges on future objects in schemas are managed
	switch g.GrantedOn {
	case ObjTpTable:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			o.isPrivilegeOnFutureObjectGrantedToReadDBRole[0] = true
		case PrvSelectErrorTable:
			o.isPrivilegeOnFutureObjectGrantedToReadDBRole[1] = true
		case PrvReferences:
			o.isPrivilegeOnFutureObjectGrantedToReadDBRole[2] = true
		}
	case ObjTpView:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			o.isPrivilegeOnFutureObjectGrantedToReadDBRole[3] = true
		case PrvReferences:
			o.isPrivilegeOnFutureObjectGrantedToReadDBRole[4] = true
		}
	case ObjTpMaterializedView:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			o.isPrivilegeOnFutureObjectGrantedToReadDBRole[5] = true
		case PrvReferences:
			o.isPrivilegeOnFutureObjectGrantedToReadDBRole[6] = true
		}
	}
	return o
}

func (o AggSchemaObjs) hasFutureGrantTo(_ Mode, grantedOn ObjType, p Privilege) bool {
	// Currently, only ModeRead privileges on future objects in schemas are managed
	switch g.GrantedOn {
	case ObjTpTable:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[0]
		case PrvSelectErrorTable:
			return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[1]
		case PrvReferences:
			return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[2]
		}
	case ObjTpView:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[3]
		case PrvReferences:
			return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[4]
		}
	case ObjTpMaterializedView:
		switch g.Privileges[0].Privilege {
		case PrvSelect:
			return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[5]
		case PrvReferences:
			return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[6]
		}
	}
	return false 
}

func (o AggSchemaObjs) setGrantTo(m Mode, g Grant) AggSchemaObjs {
	switch m {
	case ModeRead:
		switch g.GrantedOn {
		case ObjTpSchema:
			switch g.Privileges[0].Privilege {
			case PrvUsage:
				o.isPrivilegeGrantedToReadDBRole[0] = true
			case PrvMonitor:
				o.isPrivilegeGrantedToReadDBRole[1] = true
			}
		}
	case ModeWrite:
		switch g.GrantedOn {
		case ObjTpSchema:
			switch pc := g.Privileges[0]; pc.Privilege {
			case PrvCreate:
				switch pc.CreateObjType {
				case ObjTpTable:
					o.isCreateGrantedToProductWriteRole[0] = true
				case ObjTpView:
					o.isCreateGrantedToProductWriteRole[1] = true
				case ObjTpMaterializedView:
					o.isCreateGrantedToProductWriteRole[2] = true
				}
			}
		}
	}
	return o
}

func (o AggSchemaObjs) hasGrantTo(m Mode, pc PrivilegeComplete) bool {
	switch m {
	case ModeRead:
		switch pc.Privilege {
		case PrvUsage:
			return o.isPrivilegeGrantedToReadDBRole[0]
		case PrvMonitor:
			return o.isPrivilegeGrantedToReadDBRole[1]
		}
	case ModeWrite:
		switch pc.Privilege {
		case PrvCreate:
			switch pc.CreateObjType {
			case ObjTpTable:
				return o.isCreateGrantedToProductWriteRole[0]
			case ObjTpView:
				return o.isCreateGrantedToProductWriteRole[1]
			case ObjTpMaterializedView:
				return o.isCreateGrantedToProductWriteRole[2]
			}
		}
	}
	return false
}

func (o AggSchemaObjs) pushToDoFutureGrants(yield func(FutureGrant) bool, dbRole DatabaseRole, schema semantics.Ident, map[ObjType]bool mots) bool {
	if o.MatchAllObjects {
		for ot := range mots {
			prvs := []PrivilegeComplete{}
			candidatePrvs := []PrivilegeComplete{
				PrivilegeComplete{Privilege: PrvSelect},
				PrivilegeComplete{Privilege: PrvReferences},
			}
			if ot == ObjTpTable {
				candidatePrvs = append(candidatePrvs, PrivilegeComplete{Privilege: PrvSelectErrorTable})
			}
			// Note: counting on this: if you grant select error table on future tables, and you later create a
			// hybrid table, then Snowflake should not generate an error somehow that select error table is not
			// valid on a hybrid table
			for _, pc := range candidatePrvs {
				if !o.hasFutureGrantTo(ModeRead, ot, pc.Privilege) {
						prvs = append(prvs, pc)
					}
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
	prvs := []PrivilegeComplete{}
	for _, pc := range [2]PrivilegeComplete{
		PrivilegeComplete{Privilege: PrvUsage},
		PrivilegeComplete{Privilege: PrvMonitor},
	} {
		if !o.hasGrantTo(ModeRead, pc) {
			prvs = append(prvs, pc)
		}
	}
	if len(prvs) > 0 {
		if !yield(Grant{
			Privileges:        prvs,
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
