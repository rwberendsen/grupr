package snowflake

type ObjectType int

const (
	Account ObjectType = iota
	Database
	Schema
	Table
	View
	Role
	DatabaseRole
	ObjectTypeOther
)

func (ot ObjectType) String() string {
	return map[ObjectType]string{
		Account: "ACCOUNT",
		Database: "DATABASE",
		Schema: "SCHEMA",
		Table: "TABLE",
		View: "VIEW",
		Role: "ROLE",
		DatabaseRole: "DATABASE ROLE",
		ObjectTYpeOther: "OTHER",
	}[ot]
}
