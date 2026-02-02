package snowflake

type AggSchemaObjs struct {
	Objects         map[string]AggObjAttr
	MatchAllObjects bool

	// set while grants are being set
	isUsageGrantedToRead             bool
	isPrivilegeGrantedToFutureObject [2][2]bool // [ObjectType][Privilege]
}

func newAggSchemaObjs(o SchemaObjs) AggSchemaObjs {
	r := AggSchemaObjs{
		Objects:         make(map[string]ObjAttr, len(o.Objects)),
		MatchAllObjects: o.MatchAllObjects,
	}
	for k, v := range o.Objects {
		r.Objects[k] = AggObjAttr{ObjectType: v.ObjectType, Owner: v.Owner}
	}
	return r
}

func (o AggSchemaObjs) hasObject(k string) bool {
	_, ok := o.Objects[k]
	return ok
}

func (o AggSchemaObjs) setFutureGrantTo(m Mode, grantedOn ObjType, p privilege) AggSchemaObjs {
	fObjTp := func(ot ObjType) int {
		if ot == ObjTpTable {
			return 0
		}
		return 1
	}
	fPrv := func(p Privilege) int {
		if p == PrvSelect {
			return 0
		}
		return 1
	}
	switch grantedOn {
	case ObjTpTable, ObjTpView:
		switch p {
		case PrvSelect, PrvReferences:
			o.isPrivilegeGrantedToFutureObject[fObjTp(grantedOn)][fPrv(privilege)] = true
		default:
			panic("unsupported privilege on table or view")
		}
	default:
		panic("unsupported granted_on object type")
	}
	return o
}

func (o AggSchemaObjs) hasFutureGrantTo(m Mode, grantedOn ObjType, p Privilege) bool {
	fObjTp := func(ot ObjType) int {
		if ot == ObjTpTable {
			return 0
		}
		return 1
	}
	fPrv := func(p Privilege) int {
		if p == PrvSelect {
			return 0
		}
		return 1
	}
	switch grantedOn {
	case ObjTpTable, ObjTpView:
		switch p {
		case PrvSelect, PrvReferences:
			return o.isPrivilegeGrantedToFutureObject[fObjTp(grantedOn)][fPrv(privilege)]
		}
	}
	return false
}

func (o AggSchemaObjs) setGrantTo(m Mode, p Privilege) AggSchemaObjs {
	if m != ModeRead || p != PrvUsage {
		panic("not implemented")
	}
	o.isUsageGrantedToRead = true
	return o
}

func (o AggSchemaObjs) hasGrantTo(m Mode, p Privilege) {
	return m == ModeRead && p == PrvUsage && o.isUsageGranted
}

func (o AggDBObjs) pushToDoFutureGrants(yield func(FutureGrant) bool, dbRole DatabaseRole, schema string) bool {
	if o.MatchAllObjects {
		for _, ot := range [2]ObjType{ObjTpTable, ObjTpView} {
			prvs := []PrivilegeComplete{}
			for _, p := range [2]Privilege{PrvSelect, PrvReferences} {
				if !o.hasFutureGrantTo(ModeRead, ot, p) {
					prvs = append(prvs, PrivilegeComplete{Privilege: PrvUsage})
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
					GrantedToRole:     dbRole.Name,
				}) {
					return false
				}
			}
		}
	}
	return true
}

func (o AggSchemaObjs) pushToDoGrants(yield func(Grant) bool, dbRole DatabaseRole, schema string) bool {
	if !o.hasGrantTo(ModeRead, PrvUsage) {
		if !yield(Grant{
			Privileges:        []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
			GrantedOn:         ObjTpSchema,
			Database:          dbRole.Database,
			Schema:            schema,
			GrantedTo:         ObjTpDatabaseRole,
			GrantedToDatabase: dbRole.Database,
			GrantedToRole:     dbRole.Name,
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
