package snowflake

import (
	"fmt"
)

type PrivilegeComplete struct {
	Privilege        Privilege
	CreateObjectType ObjType
}

func ParsePrivilegeComplete(p string, cot string) PrivilegeComplete {
	return PrivilegeComplete{
		Privilege:        ParsePrivilege(p),
		CreateObjectType: ParseObjType(cot),
	}
}

func (p PrivilegeComplete) String() string {
	if p.Privilege == PrvCreate && p.CreateObjectType != ObjTpOther {
		return fmt.Sprintf("%s %s", p.Privilege, p.CreateObjectType)
	}
	return fmt.Sprintf("%s", p.Privilege)
}
