package snowflake

type ObjType int

const (
	ObjTpOther ObjType = iota // zero type
	ObjTpDatabase
	ObjTpDatabaseRole
	ObjTpRole
	ObjTpSchema
	ObjTpTable
	ObjTpView
)

func ParseObjType(s string) ObjType {
	return map[string]ObjType{
		"DATABASE": ObjTpDatabase,
		"DATABASE_ROLE": ObjTpDatabaseRole, // NB: in grant output we typically find DATABASE_ROLE (with underscore)
		"ROLE": ObjTpRole,
		"SCHEMA": ObjTpSchema,
		"TABLE": ObjTpTable,
		"VIEW": ObkTpView,
	}[s]
}

func (ot ObjType) String() string {
	return map[ObjType]string{
		Other: "OTHER",
		Database: "DATABASE",
		DatabaseRole: "DATABASE ROLE", // NB: in SQL queries we generally need DATABASE ROLE (with space)
		Role: "ROLE",
		Schema: "SCHEMA",
		Table: "TABLE",
		View: "VIEW",
	}[ot]
}
