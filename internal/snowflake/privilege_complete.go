package snowflake

type PrivilegeComplete struct {
	Privilege Privilege
	CreateObjectType ObjType
}

func (p PrivilegeComplete) String() string {
	if Privilege == PrvCreate {
		return p.Privilege.String() + ' ' + p.CreateObjectType.String()
	}
	return p.Privilege.String()
}
