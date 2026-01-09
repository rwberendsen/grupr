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
		"ACCOUNT": ObjTpAccount,
		"DATABASE": ObjTpDatabase,
		"DATABASE ROLE": ObjTpDatabaseRole,
		"ROLE": ObjTpRole,
		"SCHEMA": ObjTpSchema,
		"TABLE": ObjTpTable,
		"VIEW": ObkTpView,
	}[s]
}

func (ot ObjType) String() string {
	return map[ObjType]string{
		Other: "OTHER",
		Account: "ACCOUNT",
		Database: "DATABASE",
		DatabaseRole: "DATABASE ROLE",
		Role: "ROLE",
		Schema: "SCHEMA",
		Table: "TABLE",
		View: "VIEW",
	}[ot]
}
