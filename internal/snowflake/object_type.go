package snowflake

type ObjectType int

const (
	Account ObjectType = iota
	Database
	Schema
	Table
	View
	ObjectTypeOther
)
