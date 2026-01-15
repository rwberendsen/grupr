package snowflake

import (
	"context"
	"csv"
	"database/sql"
	"fmt"
	"iter"
	"maps"
	"strings"
)

type Grant struct {
	Privileges 		map[PrivilegeComplete]struct{}
	GrantedOn 		ObjType
	Database		string
	Schema			string
	Object			string
	// Note how Role is not the grantee, but the object on which a privilege is granted;
	// it can be the name of a database role, if GrantedOn == ObjTpDatabaseRole
	Role			string 
	GrantedTo		ObjType
	GrantedToDatabase	string
	GrantedToRole		string
	GrantOption		bool // TODO: if we re-grant the same grant with a different grant option, does it get overwritten? Could be a way to correct such mishaps
	GrantedBy		string
}

func (g Grant) buildSQLGrant() string {
	var toClause string
	switch g.GrantedTo {
		case ObjTypeRole:
			toClause = fmt.Sprintf(`TO ROLE %s`, quoteIdentifier(g.GrantedToRole))
		case ObjTypeDatabaseRole:
			toClause = fmt.Sprintf(`TO DATABASE ROLE %s.%s`, quoteIdentifier(g.GrantedToDatabase), quoteIdentifier(g.GrantedToRole))
		default:
			panic("Not implemented")
	}

	// GRANT ROLE ... / GRANT DATABASE ROLE ...
	switch g.GrantedOn {
	case ObjTpRole:
		return fmt.Sprintf(`GRANT ROLE %s %s`, quoteIdentifier(g.Role), toClause)
	case ObjTpDatabaseRole:
		return fmt.Sprintf(`GRANT DATABASE ROLE %s.%s %s`, quoteIdentifier(g.Database), quoteIdentifier(g.Role), toClause)
	}
	
	// GRANT <privileges> ... TO ROLE
	privilegeClause := strings.Join(g.Privileges.Keys(), `, `)
	
	var onClause string
	switch g.GrantedOn {
	case ObjTpDatabase:
		onClause = fmt.Sprintf(`ON %v %s`, g.GrantedOn, quoteIdentifier(g.Database))
	case ObjTpSchema:
		onClause = fmt.Sprintf(`ON %v %s.%s`, g.GrantedOn, quoteIdentifier(g.Database), quoteIdentifier(g.Schema))
	case ObjTpTable || ObjTypeView:
		onClause = fmt.Sprintf(`ON %v %s.%s.%s`, g.GrantedOn, quoteIdentifier(g.Database), quoteIdentifier(g.Schema), quoteIdentifier(g.Object))
	default:
		panic("Not implemented")
	}
	return fmt.Sprintf(`GRANT %s %s %s`, privilegeClause, onClause, toClause)
}

func newGrant(privilege string, createObjType string, grantedOn string, name string, grantedTo ObjType,
		grantedToDatabase string, grantedToRole string, grantOption bool, grantedBy string) (Grant, error) {
	g := Grant{
		Privilege: ParsePrivilege(privilege),
		CreateObjectType: ParseObjType(createObjType),
		GrantedOn: PparseObjType(grantedOn),
		GrantedTo: grantedTo,
		GrantedToDatabase: grantedToDatabase,
		GrantedToRole: grantedToRole,
		GrantOption: grantOption,
		GrantedBy: grantedBy,
	}
	fpr := map[ObjType]int{
		ObjTpAccount: 1,
		ObjTpDatabase: 1,
		ObjTpDatabaseRole: 2,
		ObjTpRole: 1,
		ObjTpSchema: 2,
		ObjTpTable: 3,
		ObjTpView: 3,
	}
	r := csv.NewReader(strings.NewReader(name) // handles quoted fields as they appear in name
	r.Comma = '.'
	r.FieldsPerRecord = fpr[g.GrantedOn]
	rec, err := r.Read()
	if err != nil { return g, err }
	_, err = r.Read(); err != io.EOF { return g, err } // more than one record
	switch g.GrantedOn {
	case ObjTpDatabase: 
		g.Database = rec[0]
	case ObjTpDatabaseRole:
		g.Database = rec[0]
		g.Role = rec[1]
	case ObjTpRole:
		g.Role = rec[0]
	case ObjTpSchema:
		g.Database = rec[0]
		g.Schema = rec[1]
	case ObjTpTable, ObjTpView:
		g.Database = rec[0]
		g.Schema = rec[1]
		g.Object = rec[2]
	}
	return g, nil
}

func QueryGrantsToRoleFiltered(ctx context.Context, conn *sql.DB, role string,
		match map[Grant]struct{}, notMatch map[Grant]struct{}) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, conn, "", role, match, notMatch, 0)
}

func QueryGrantsToDBRoleFiltered(ctx context.Context, conn *sql.DB, db string, role string,
		match map[Grant]struct{}, notMatch map[Grant]struct{}) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, conn, db, role, match, notMatch, 0)
}

func QueryGrantsToRole(ctx context.Context, conn *sql.DB, role string) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, conn, "", role, nil, nil, 0)
}

func QueryGrantsToDBRole(ctx context.Context, conn *sql.DB, db string, role string) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, conn, db, role, nil, nil, 0)
}

func QueryGrantsToRoleFilteredLimit(ctx context.Context, conn *sql.DB, role string,
		match map[Grant]struct{}, notMatch map[Grant]struct{}, limit int) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, conn, "", role, match, notMatch, limit)
}

func (g Grant) buildSQLFilter(g Grant) (string, int) {
	// zero values
	var privilege Privilege
	var createObjectType ObjType
	var grantedOn ObjType

	clauses := []string{}
	if g.Privilege != privilege {
		clauses = append(clauses, fmt.Sprintf("privilege = '%v'", g.Privilege))	
		if g.Privilege == PrvCreate && g.CreateObjectType != createObjectType {
			clauses = append(clauses, fmt.Sprintf("create_object_type = '%v'", g.CreateObjectType))
		}
	}
	if g.GrantedOn != grantedOn {
		clauses = append(clauses, fmt.Sprintf("granted_on = '%v'", g.GrantedOn))
	}
	return strings.Join(clauses, " AND "), len(clauses)
}

func buildSQLGrants(grants map[Grant]struct{}) (string, int) {
	clauses = []string{}
	for g := range grants {
		s, l := buildSQLFilter(g)
		if l > 0 {
			clauses = append(clauses, s)
		}
	}	
	return strings.Join(clauses, " OR\n"), len(clauses)
}

func buildSLQMatch(match map[Grant]struct{}, notMatch map[Grant]struct{}) (string, int) {
	clauses := []string{}
	if match != nil {
		s, l := buildSQLGrants(match)
		if l > 0 {
			clauses = append(clauses, s)
		}
	}
	if notMatch != nil {
		s, l := buildSQLGrants(notMatch)
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

func buildSQLQueryGrants(db string, role string, match map[Grant]struct{}, notMatch map[Grant]struct{}, limit int) string {
	// fetch grants for DATABASE ROLE if needed, rather than ROLE
	var dbClause string
	granteeName := quoteIdentifier(role)
	if db != "" {
		dbClause = `DATABASE `
		// Note how we quote the db identifier, other processes created it and may have used special characters.
		granteeName = fmt.Sprintf(`%s.%s`, quoteIdentifier(db), granteeName)
	}

	var whereClause string
	clauseStr, nClauses := buildSQLMatch(match, notMatch)
	if nClauses > 0 {
		whereClause = fmt.Sprintf("\nWHERE\n  %s", strings.ReplaceAll(clauseStr, "\n", "\n  "))
	}

	var sql string
	sql := fmt.Sprintf(`SHOW GRANTS TO %sROLE IDENTIFIER('%s')
->> SELECT
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ') THEN 'CREATE'
    ELSE "privilege"
    END AS privilege
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ') THEN SUBSTR("privilege", 8)
    ELSE NULL
    END AS create_object_type
  , "granted_on"	AS granted_on
  , "name"		AS name
  , "grant_option"	AS grant_option
  , "granted_by"	AS granted_by
FROM $1%s`, dbClause, granteeName, whereClause)
	
	if limit > 0 {
		sql += fmt.Sprintf("\nLIMIT %d", limit)
	}

	return
}

func queryGrantsToRole(ctx context.Context, conn *sql.DB, db string, role string,
		match map[Grant]struct{}, notMatch map[Grant]struct{}, limit int) iter.Seq2[Grant, error] {
	// Note that both db and string will be quoted before going to Snowflake, so
	// if the names in Snowflake are upper case, present them here in upper case, too.
	grantedTo := ObjTpRole
	if db != "" {
		grantedTo = ObjTpDatabaseRole
	}
	sql := buildSQLQueryGrants(db, role, match, notMatch, limit)
	return func(yield func(Grant, error) bool) {
		rows, err := conn.QueryContext(ctx, sql, param)
		defer rows.Close()
		if err != nil { 
			if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
				err = ErrObjectNotExistOrAuthorized
			}
			yield(Grant{}, err)
			return
		}
		for rows.Next() {
			var privilege string
			var createObjectType string
			var grantedOn string
			var name string
			var grantOption bool
			var grantedBy string
			if err = rows.Scan(&privilege, &createObjectType, &grantedOn, &name, &grantOption, &grantedBy); err != nil {
				yield(Grant{}, err)
				return
			}
			g, err := newGrant(privilege, createObjectType, grantedOn, name, grantedTo, db, role, grantOption, grantedBy)
			if err != nil {
				yield(Grant{}, err}
			}
			if !yield(g, nil)
				return
			}
		}
		if err = rows.Err(); err != nil {
			yield(Grant{}, err)
			return
		}
	}
}


func DoGrants(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq2[Grant, error]) error {
	buf := [cnf.StmtBatchSize]string{}
	i := 0
	for g, err := range grants {
		if err != nil { return err }
		buf[i] := g.buildSQL()
	}
}
