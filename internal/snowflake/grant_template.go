package snowflake

type GrantTemplate struct {
	PrivilegeComplete
	GrantedOn ObjType
	GrantedRoleStartsWithPrefix *bool
}
