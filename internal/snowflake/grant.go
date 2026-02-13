package snowflake

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/rwberendsen/grupr/internal/util"
)

type Grant struct {
	Privileges                    []PrivilegeComplete
	GrantedOn                     ObjType
	Database                      string
	Schema                        string
	Object                        string
	GrantedRole                   string
	GrantedRoleStartsWithPrefix   bool
	GrantedTo                     ObjType
	GrantedToDatabase             string
	GrantedToRole                 string
	GrantedToRoleStartsWithPrefix bool
	GrantOption                   bool // TODO: if we re-grant the same grant with a different grant option, does it get overwritten? Could be a way to correct such mishaps
	GrantedBy                     string
	// TODO: consider using struct packing to align better and have more compact memory layout
}

func (g Grant) buildSQLGrant(revoke bool) string {
	verb := `GRANT`
	preposition := `TO`
	if revoke {
		verb = `REVOKE`
		preposition = `FROM`
	}
	var granteeClause string
	switch g.GrantedTo {
	case ObjTpRole:
		granteeClause = fmt.Sprintf(`ROLE %s`, quoteIdentifier(g.GrantedToRole))
	case ObjTpDatabaseRole:
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
	privilegeClause := strings.Join(util.FmtSliceElements[PrivilegeComplete](g.Privileges...), `, `)

	var objectClause string
	switch g.GrantedOn {
	case ObjTpDatabase:
		objectClause = fmt.Sprintf(`%v %s`, g.GrantedOn, quoteIdentifier(g.Database))
	case ObjTpSchema:
		objectClause = fmt.Sprintf(`%v %s.%s`, g.GrantedOn, quoteIdentifier(g.Database), quoteIdentifier(g.Schema))
	case ObjTpTable, ObjTpView:
		objectClause = fmt.Sprintf(`%v %s.%s.%s`, g.GrantedOn, quoteIdentifier(g.Database), quoteIdentifier(g.Schema), quoteIdentifier(g.Object))
	default:
		panic("Not implemented")
	}
	return fmt.Sprintf(`%s %s ON %s %s %s`, verb, privilegeClause, objectClause, preposition, granteeClause)
}

func newGrant(privilege string, createObjType string, grantedOn string, name string, grantedRoleStartsWithPrefix bool, grantedTo ObjType,
	grantedToDatabase string, grantedToRole string, grantedToRoleStartsWithPrefix bool, grantOption bool, grantedBy string) (Grant, error) {
	g := Grant{
		Privileges:                    []PrivilegeComplete{ParsePrivilegeComplete(privilege, createObjType)},
		GrantedOn:                     ParseObjType(grantedOn),
		GrantedRoleStartsWithPrefix:   grantedRoleStartsWithPrefix,
		GrantedTo:                     grantedTo,
		GrantedToDatabase:             grantedToDatabase,
		GrantedToRole:                 grantedToRole,
		GrantedToRoleStartsWithPrefix: grantedToRoleStartsWithPrefix,
		GrantOption:                   grantOption,
		GrantedBy:                     grantedBy,
	}
	fpr := map[ObjType]int{
		ObjTpAccount:      1,
		ObjTpDatabase:     1,
		ObjTpDatabaseRole: 2,
		ObjTpRole:         1,
		ObjTpSchema:       2,
		ObjTpTable:        3,
		ObjTpView:         3,
	}
	r := csv.NewReader(strings.NewReader(name)) // handles quoted fields as they appear in name
	r.Comma = '.'
	r.FieldsPerRecord = fpr[g.GrantedOn]
	rec, err := r.Read()
	if err != nil {
		return g, err
	}
	if _, err = r.Read(); err != io.EOF {
		return g, err
	} // more than one record
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
	grantedToRoleStartsWithPrefix bool, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, "", role, grantedToRoleStartsWithPrefix, match, notMatch, limit)
}

func QueryGrantsToDBRoleFilteredLimit(ctx context.Context, cnf *Config, conn *sql.DB, db string, role string,
	grantedToRoleStartsWithPrefix bool, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, db, role, grantedToRoleStartsWithPrefix, match, notMatch, limit)
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
	clauseStr, nClauses := buildSQLMatchNotMatchGrantTemplates(match, notMatch)
	if nClauses > 0 {
		whereClause = fmt.Sprintf("\nWHERE\n  %s", strings.ReplaceAll(clauseStr, "\n", "\n  "))
	}

	query := fmt.Sprintf(`SHOW GRANTS TO %sROLE IDENTIFIER('%s')
->> SELECT
    CASE
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
		query += fmt.Sprintf("\nLIMIT %d", limit)
	}

	return query
}

func queryGrantsToRole(ctx context.Context, cnf *Config, conn *sql.DB, db string, role string,
	grantedToRoleStartsWithPrefix bool, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	// Note that both db and string will be quoted before going to Snowflake, so
	// if the names in Snowflake are upper case, present them here in upper case, too.
	grantedTo := ObjTpRole
	if db != "" {
		grantedTo = ObjTpDatabaseRole
	}
	query := buildSQLQueryGrants(db, role, match, notMatch, cnf.ObjectPrefix, limit)
	return func(yield func(Grant, error) bool) {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
				err = ErrObjectNotExistOrAuthorized
			}
			yield(Grant{}, err)
			return
		}
		defer rows.Close()
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
				yield(Grant{}, err)
			}
			if !yield(g, nil) {
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
			if err := runMultipleSQL(ctx, cnf, conn, strings.Join(buf, ";"), i); err != nil {
				return err
			}
			i = 0
		}
		buf[i] = g.buildSQLGrant(revoke)
		i++
	}
	if i > 0 {
		if err := runMultipleSQL(ctx, cnf, conn, strings.Join(buf[0:i], ";"), i); err != nil {
			return err
		}
	}
	return nil
}

func doGrantsSkipErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant], revoke bool) error {
	for g := range grants {
		if err := runSQL(ctx, cnf, conn, g.buildSQLGrant(revoke)); err != nil && err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
	return nil
}
