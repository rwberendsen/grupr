package snowflake

type PrivilegeComplete struct {
	Privilege Privilege
	CreateObjectType ObjType
}

func ParsePrivilegeComplete(p string, cot string) PrivilegeComplete {
	return PrivilegeComplete{
		Privilege: ParsePrivilege(p),
		CreateObjectType: ParseObjType(cot),
	}
}

func (p PrivilegeComplete) String() string {
	if Privilege == PrvCreate {
		return p.Privilege.String() + ' ' + p.CreateObjectType.String()
	}
	return p.Privilege.String()
}
