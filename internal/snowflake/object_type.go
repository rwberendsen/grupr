package snowflake

type ObjType int

const (
	ObjTpOther ObjType = iota // zero type
	ObjTpAccount
	ObjTpDatabase
	ObjTpDatabaseRole
	ObjTpRole
	ObjTpSchema
	ObjTpTable
	ObjTpView
)

func ParseObjType(s string) ObjType {
	return map[string]ObjType{
		"ACCOUNT":       ObjTpAccount,
		"DATABASE":      ObjTpDatabase,
		"DATABASE_ROLE": ObjTpDatabaseRole, // NB: in grant output we typically find DATABASE_ROLE (with underscore)
		"ROLE":          ObjTpRole,
		"SCHEMA":        ObjTpSchema,
		"TABLE":         ObjTpTable,
		"VIEW":          ObkTpView,
	}[s]
}

func (ot ObjType) String() string {
	return map[ObjType]string{
		ObjTpOther:        "OTHER",
		ObjTpAccount:      "ACCOUNT",
		ObjTpDatabase:     "DATABASE",
		ObjTpDatabaseRole: "DATABASE_ROLE", 
		ObjTpRole:         "ROLE",
		ObjTpSchema:       "SCHEMA",
		ObjTpTable:        "TABLE",
		ObjTpView:         "VIEW",
	}[ot]
}

func (ot ObjType) getIdxObjectLevel() int {
	switch ot {
	case ObjTpTable:
		return 0
	case ObjTpView:
		return 1
	default:
		panic("not an object living within a schema or not yet implemented")
	}
}
