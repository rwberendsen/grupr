package snowflake

import (
	"fmt"
	"strings"
)

type GrantTemplate struct {
	PrivilegeComplete
	GrantedOn                   ObjType
	GrantedRoleStartsWithPrefix *bool
}

func (g GrantTemplate) buildSQLFilter() (string, int) {
	clauses := []string{}
	if g.Privilege != PrvOther {
		clauses = append(clauses, fmt.Sprintf("privilege = '%v'", g.Privilege))
		if g.Privilege == PrvCreate && g.CreateObjectType != ObjTpOther {
			clauses = append(clauses, fmt.Sprintf("create_object_type = '%v'", g.CreateObjectType))
		}
	}
	if g.GrantedOn != ObjTpOther {
		clauses = append(clauses, fmt.Sprintf("granted_on = '%v'", g.GrantedOn))
	}
	if (g.GrantedOn == ObjTpRole || g.GrantedOn == ObjTpDatabaseRole) && g.GrantedRoleStartsWithPrefix != nil {
		clauses = append(clauses, "granted_role_starts_with_prefix")
	}
	return strings.Join(clauses, " AND "), len(clauses)
}

func buildSQLMatchGrantTemplates(grants map[GrantTemplate]struct{}) (string, int) {
	clauses := []string{}
	for g := range grants {
		s, l := g.buildSQLFilter()
		if l > 0 {
			clauses = append(clauses, s)
		}
	}
	return strings.Join(clauses, " OR\n"), len(clauses)
}

func buildSQLMatchNotMatchGrantTemplates(match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) (string, int) {
	clauses := []string{}
	if match != nil {
		s, l := buildSQLMatchGrantTemplates(match)
		if l > 0 {
			clauses = append(clauses, s)
		}
	}
	if notMatch != nil {
		s, l := buildSQLMatchGrantTemplates(notMatch)
		if l > 0 {
			clauses = append(clauses, fmt.Sprintf("NOT (%s)", s))
		}
	}
	if len(clauses) == 2 {
		for i, clause := range clauses {
			clauses[i] = fmt.Sprintf("(%s)", clause)
		}
	}
	return strings.Join(clauses, "\nAND\n"), len(clauses)
}
