package snowflake

type GrantTemplate struct {
	Privilege Privilege
	CreateObjectType ObjType
	GrantedOn ObjType
	GrantedRoleStartsWithPrefix *bool
}
