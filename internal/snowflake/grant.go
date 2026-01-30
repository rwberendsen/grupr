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
	Privileges 			map[PrivilegeComplete]struct{}
	GrantedOn 			ObjType
	Database			string
	Schema				string
	Object				string
	GrantedRole			string 
	GrantedRoleStartsWithPrefix	bool 
	GrantedTo			ObjType
	GrantedToDatabase		string
	GrantedToRole			string
	GrantedToRoleStartsWithPrefix	bool
	GrantOption			bool // TODO: if we re-grant the same grant with a different grant option, does it get overwritten? Could be a way to correct such mishaps
	GrantedBy			string
	// TODO: consider using struct packing to align better and have more compact memory layout
}

func (g Grant) buildSQLGrant(revoke bool) string {
	verb := 'GRANT'
	preposition := 'TO'
	if revoke {
		verb = 'REVOKE'
		prepostition = 'FROM'
	}
	var granteeClause string
	switch g.GrantedTo {
		case ObjTypeRole:
			granteeClause = fmt.Sprintf(`ROLE %s`, quoteIdentifier(g.GrantedToRole))
		case ObjTypeDatabaseRole:
			granteeClause = fmt.Sprintf(`DATABASE ROLE %s.%s`, quoteIdentifier(g.GrantedToDatabase), quoteIdentifier(g.GrantedToRole))
		default:
			panic("Not implemented")
	}

	// GRANT ROLE ... / GRANT DATABASE ROLE ...
	switch g.GrantedOn {
	case ObjTpRole:
		return fmt.Sprintf(`%s ROLE %s %s %s`, verb, quoteIdentifier(g.GrantedRole), preposition, granteeClause)
	case ObjTpDatabaseRole:
		return fmt.Sprintf(`%s DATABASE ROLE %s.%s %s`, verb, quoteIdentifier(g.Database), quoteIdentifier(g.GrantedRole), preposition, granteeClause)
	}
	
	// GRANT <privileges> ... TO ROLE
	privilegeClause := strings.Join(maps.Keys(g.Privileges), `, `)
	
	var objectClause string
	switch g.GrantedOn {
	case ObjTpDatabase:
		objectClause = fmt.Sprintf(`%v %s`, g.GrantedOn, quoteIdentifier(g.Database))
	case ObjTpSchema:
		objectClause = fmt.Sprintf(`%v %s.%s`, g.GrantedOn, quoteIdentifier(g.Database), quoteIdentifier(g.Schema))
	case ObjTpTable, ObjTypeView:
		objectClause = fmt.Sprintf(`%v %s.%s.%s`, g.GrantedOn, quoteIdentifier(g.Database), quoteIdentifier(g.Schema), quoteIdentifier(g.Object))
	default:
		panic("Not implemented")
	}
	return fmt.Sprintf(`%s %s %s %s %s`, verb, privilegeClause, objectClause, preposition, granteeClause)
}

func newGrant(privilege string, createObjType string, grantedOn string, name string, grantedRoleStartsWithPrefix bool, grantedTo ObjType,
		grantedToDatabase string, grantedToRole string, grantedToRoleStartsWithPrefix bool, grantOption bool, grantedBy string) (Grant, error) {
	g := Grant{
		Privileges: map[PrivilegeComplete]struct{}{ParsePrivilegeComplete(privilege, createObjType): {}},
		GrantedOn: ParseObjType(grantedOn),
		GrantedRoleStartsWithPrefix: grantedRoleStartsWithPrefix,
		GrantedTo: grantedTo,
		GrantedToDatabase: grantedToDatabase,
		GrantedToRole: grantedToRole,
		GrantedToRoleStartsWithPrefix: grantedToRoleStartsWithPrefix,
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
		g.GrantedRole = rec[1]
	case ObjTpRole:
		g.GrantedRole = rec[0]
	case ObjTpSchema:
		g.Database = rec[0]
		g.Schema = rec[1]
	case ObjTpTable, ObjTpView:
		g.Database = rec[0]
		g.Schema = rec[1]
		g.Object = rec[2]
	default:
		return g, fmt.Errorf("unsupported granted_on object type for grant")
	}
	return g, nil
}

func QueryGrantsToRoleFiltered(ctx context.Context, cnf *Config, conn *sql.DB, role string,
		grantedToRoleStartsWithPrefix bool, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, "", role, grantedToRoleStartsWithPrefix, match, notMatch, 0)
}

func QueryGrantsToDBRoleFiltered(ctx context.Context, cnf *Config, conn *sql.DB, db string, role string,
		grantedToRoleStartsWithPrefix bool, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, db, role, grantedToRoleStartsWithPrefix, match, notMatch, 0)
}

func QueryGrantsToRole(ctx context.Context, cnf *Config, conn *sql.DB, role string, grantedToRoleStartsWithPrefix bool) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, "", role, grantedToRoleStartsWithPrefix, nil, nil, 0)
}

func QueryGrantsToDBRole(ctx context.Context, cnf *Config, conn *sql.DB, db string, role string, grantedToRoleStartsWithPrefix bool) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, db, role, grantedToRoleStartsWithPrefix, nil, nil, 0)
}

func QueryGrantsToRoleFilteredLimit(ctx context.Context, cnf *Config, conn *sql.DB, role string,
		grantedToRoleStartsWithPrefix, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, "", role, grantedToRoleStartsWithPrefix, match, notMatch, limit)
}

func (g Grant) buildSQLFilter(g GrantTemplate) (string, int) {
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

func buildSQLGrants(grants map[GrantTemplate]struct{}) (string, int) {
	clauses = []string{}
	for g := range grants {
		s, l := buildSQLFilter(g)
		if l > 0 {
			clauses = append(clauses, s)
		}
	}	
	return strings.Join(clauses, " OR\n"), len(clauses)
}

func buildSLQMatch(match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) (string, int) {
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

func buildSQLQueryGrants(db string, role string, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, grantedRolePrefix string, limit int) string {
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
    WHEN STARTSWITH("privilege", 'CREATE ')
    THEN 'CREATE'
    ELSE "privilege"
    END AS privilege
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ')
    THEN SUBSTR("privilege", 8)
    ELSE NULL
    END AS create_object_type
  , "granted_on"	AS granted_on
  , "name"		AS name
  , CASE
    WHEN granted_on IN ('ROLE', 'DATABASE_ROLE')
    THEN STARTSWITH(name, '%s')
    ELSE NULL
    END AS granted_role_starts_with_prefix
  , "grant_option"	AS grant_option
  , "granted_by"	AS granted_by
FROM $1%s`, dbClause, granteeName, grantedRolePrefix, whereClause)
	
	if limit > 0 {
		sql += fmt.Sprintf("\nLIMIT %d", limit)
	}

	return
}

func queryGrantsToRole(ctx context.Context, cnf *Config, conn *sql.DB, db string, role string,
		grantedToRoleStartsWithPrefix, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	// Note that both db and string will be quoted before going to Snowflake, so
	// if the names in Snowflake are upper case, present them here in upper case, too.
	grantedTo := ObjTpRole
	if db != "" {
		grantedTo = ObjTpDatabaseRole
	}
	sql := buildSQLQueryGrants(db, role, match, notMatch, cnf.ObjectPrefix, limit)
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
			var grantedRoleStartsWithPrefix bool
			var grantOption bool
			var grantedBy string
			if err = rows.Scan(&privilege, &createObjectType, &grantedOn, &name, &grantedRoleStartsWithPrefix, &grantOption, &grantedBy); err != nil {
				yield(Grant{}, err)
				return
			}
			// NB: the caller decides which role to query, and therefore knows if the role starts with the prefix from Cnf
			g, err := newGrant(privilege, createObjectType, grantedOn, name, grantedRoleStartsWithPrefix, grantedTo, db, role, grantedToRoleStartsWithPrefix, grantOption, grantedBy)
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

func DoGrants(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrants(ctx, cnf, conn, grants, false)	
}

func DoGrantsSkipErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrants(ctx, cnf, conn, grants, false)	
}

func DoRevokes(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrants(ctx, cnf, conn, grants, true)	
}

func DoRevokesSkipErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrants(ctx, cnf, conn, grants, true)	
}

func doGrants(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant], revoke bool) error {
	// Runs grant statements in batches
	buf := make([]string, cnf.StmtBatchSize)
	i := 0
	for g := range grants {
		if i == cnf.StmtBatchSize {
			if err := runMultipleSQL(ctx, cnf, conn, slices.Join(buf, ";"), i); err != nil { return err }
			i = 0
		}
		buf[i] := g.buildSQLGrant(revoke)
		i++
	}
	if i > 0 {
		if err := runMultipleSQL(ctx, cnf, conn, slices.Join(buf[0:i], ";"), i); err != nil { return err }
	}
	return nil
}

func doGrantsSkipErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant], revoke bool) error {
	for g := range grants {
		if err := runMultipleSQL(ctx, cnf, conn, g.buildSQLGrant(revoke)); err != nil && err != ErrObjectNotExistOrAuthorized { return err }
	}
	return nil
}
