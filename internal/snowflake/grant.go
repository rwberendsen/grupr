package snowflake

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type Grant struct {
	Privileges                []PrivilegeComplete
	GrantedOn                 ObjType
	Database                  semantics.Ident
	Schema                    semantics.Ident
	Object                    semantics.Ident
	GrantedRole               semantics.Ident
	GrantedRoleIsGruprManaged *bool
	GrantedTo                 ObjType
	GrantedToDatabase         semantics.Ident
	GrantedToName             semantics.Ident
	GrantOption               bool // TODO: if we re-grant the same grant with a different grant option, does it get overwritten? Could be a way to correct such mishaps
	GrantedBy                 semantics.Ident
	// TODO: consider using struct packing to align better and have more compact memory layout
}

func (g Grant) buildSQLGrant(revoke bool) string {
	verb := `GRANT`
	preposition := `TO`
	if revoke {
		verb = `REVOKE`
		preposition = `FROM`
	}
	granteeClause := fmt.Sprintf(`%s IDENTIFIER($$%s$$)`, g.GrantedTo, g.GrantedTo.FQN(g.GrantedToDatabase, semantics.Ident(""), g.GrantedToName))

	// GRANT ROLE ... / GRANT DATABASE ROLE ...
	switch g.GrantedOn {
	case ObjTpRole, ObjTpDatabaseRole:
		return fmt.Sprintf(`%s %s IDENTIFIER($$%s$$) %s %s`, verb, g.GrantedOn, g.GrantedOn.FQN(g.Database, semantics.Ident(""), g.GrantedRole), preposition, granteeClause)
	}

	// GRANT <privileges> ... TO ROLE
	privilegeClause := strings.Join(util.FmtSliceElements[PrivilegeComplete](g.Privileges...), `, `)
	var modifierClause string
	if len(g.Privileges) == 1 && g.Privileges[0].Privilege == PrvOwnership {
		modifierClause = ` COPY CURRENT GRANTS`
	}

	objectClause := fmt.Sprintf(`%v IDENTIFIER($$%s$$)`, g.GrantedOn, g.GrantedOn.FQN(g.Database, g.Schema, g.Object))
	return fmt.Sprintf(`%s %s ON %s %s %s%s`, verb, privilegeClause, objectClause, preposition, granteeClause, modifierClause)
}

func newGrantToRole(privilege string, createObjType string, grantedOn string, name string, grantedRoleIsGruprManaged *bool, grantedTo ObjType,
	grantedToDatabase semantics.Ident, grantedToName semantics.Ident, grantOption bool, grantedBy semantics.Ident) (Grant, error) {
	g := Grant{
		Privileges:                []PrivilegeComplete{ParsePrivilegeComplete(privilege, createObjType)},
		GrantedOn:                 ParseObjTypeFromRecord(grantedOn),
		GrantedRoleIsGruprManaged: grantedRoleIsGruprManaged,
		GrantedTo:                 grantedTo,
		GrantedToDatabase:         grantedToDatabase,
		GrantedToName:             grantedToName,
		GrantOption:               grantOption,
		GrantedBy:                 grantedBy,
	}
	r := csv.NewReader(strings.NewReader(name)) // handles quoted fields as they appear in name
	r.Comma = '.'
	rec, err := r.Read()
	if err != nil {
		return g, err
	}
	if _, err = r.Read(); err != io.EOF {
		return g, err
	} // more than one record
	switch g.GrantedOn {
	case ObjTpDatabase:
		g.Database = semantics.Ident(rec[0])
	case ObjTpDatabaseRole:
		g.Database = semantics.Ident(rec[0])
		g.GrantedRole = semantics.Ident(rec[1])
	case ObjTpRole:
		g.GrantedRole = semantics.Ident(rec[0])
	case ObjTpSchema:
		g.Database = semantics.Ident(rec[0])
		g.Schema = semantics.Ident(rec[1])
	case ObjTpTable, ObjTpView, ObjTpMaterializedView:
		g.Database = semantics.Ident(rec[0])
		g.Schema = semantics.Ident(rec[1])
		g.Object = semantics.Ident(rec[2])
	case ObjTpWarehouse:
		g.Object = semantics.Ident(rec[0])
	case ObjTpOther:
		// We don't know anything about how to interpret 'name',
		// We'll just leave blanks in g.Database, g.Schema, g.Object, etc.
	default:
		return g, fmt.Errorf("unsupported granted_on object type for grant")
	}
	return g, nil
}

func newGrantOfRole(role semantics.Ident, granteeName semantics.Ident, grantedBy semantics.Ident) Grant {
	return Grant{
		Privileges:                []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
		GrantedOn:                 ObjTpRole,
		GrantedRole:               role,
		GrantedRoleIsGruprManaged: util.NewTrue(), // only used for product dtap roles at this point
		GrantedTo:                 ObjTpUser,
		GrantedToName:             granteeName,
		GrantedBy:                 grantedBy,
	}
}

func newExternalGrantOnObject(grantedTo string, granteeName string, grantedOn ObjType, db semantics.Ident, schema semantics.Ident, obj semantics.Ident) (Grant, error) {
	g := Grant{
		Privileges:                []PrivilegeComplete{PrivilegeComplete{Privilege: PrvAll}},
		Database:                  db,
		Schema:                    schema,
		Object:                    obj,
		GrantedOn:                 grantedOn,
		GrantedTo:                 ParseObjTypeFromRecord(grantedTo),
	}
	r := csv.NewReader(strings.NewReader(granteeName)) // handles quoted fields as they appear in name
	r.Comma = '.'
	rec, err := r.Read()
	if err != nil {
		return g, err
	}
	if _, err = r.Read(); err != io.EOF {
		return g, err
	} // more than one record
	switch g.GrantedTo {
	case ObjTpUser, ObjTpRole:
		g.GrantedToName = semantics.Ident(rec[0])
	case ObjTpDatabaseRole:
		g.Database = semantics.Ident(rec[0])
		g.GrantedToName = semantics.Ident(rec[1])
	default:
		return g, fmt.Errorf("unsupported granted_to object type for grant")
	}
	return g, nil
}

func QueryGrantsToRoleFiltered(ctx context.Context, cnf *Config, conn *sql.DB, role semantics.Ident,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, "", role, match, notMatch, 0)
}

func QueryGrantsToDBRoleFiltered(ctx context.Context, cnf *Config, conn *sql.DB, db semantics.Ident, role semantics.Ident,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, db, role, match, notMatch, 0)
}

func QueryGrantsToRole(ctx context.Context, cnf *Config, conn *sql.DB, role semantics.Ident) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, "", role, nil, nil, 0)
}

func QueryGrantsToDBRole(ctx context.Context, cnf *Config, conn *sql.DB, db semantics.Ident, role semantics.Ident) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, db, role, nil, nil, 0)
}

func QueryGrantsToRoleFilteredLimit(ctx context.Context, cnf *Config, conn *sql.DB, role semantics.Ident,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, "", role, match, notMatch, limit)
}

func QueryGrantsToDBRoleFilteredLimit(ctx context.Context, cnf *Config, conn *sql.DB, db semantics.Ident, role semantics.Ident,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	return queryGrantsToRole(ctx, cnf, conn, db, role, match, notMatch, limit)
}

func buildSQLQueryGrantsToRole(db semantics.Ident, role semantics.Ident, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{},
	gruprRole semantics.Ident, limit int) string {
	// fetch grants for DATABASE ROLE if needed, rather than ROLE
	var dbClause string
	granteeName := fmt.Sprintf(`%s`, role)
	if db != "" {
		dbClause = `DATABASE `
		// Note how we quote the db identifier, other processes created it and may have used special characters.
		granteeName = fmt.Sprintf(`%s.%s`, db, role)
	}

	var whereClause string
	clauseStr, nClauses := buildSQLMatchNotMatchGrantTemplates(match, notMatch)
	if nClauses > 0 {
		whereClause = fmt.Sprintf("\nWHERE\n  %s", strings.ReplaceAll(clauseStr, "\n", "\n  "))
	}

	query := fmt.Sprintf(`SHOW GRANTS TO %sROLE IDENTIFIER($$%s$$)
->> SELECT
    CASE
    WHEN STARTSWITH("privilege", 'CREATE ')
    THEN 'CREATE'
    ELSE "privilege"
    END AS privilege
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ')
    THEN SUBSTR("privilege", 8)
    ELSE '' 
    END AS create_object_type
  , "granted_on"	AS granted_on
  , "name"		AS name
  , CASE
    WHEN granted_on IN ('ROLE', 'DATABASE_ROLE')
    THEN "granted_by" = '%s'
    ELSE NULL
    END AS granted_role_is_grupr_managed
  , "grant_option"	AS grant_option
  , "granted_by"	AS granted_by
FROM $1%s`, dbClause, granteeName, string(gruprRole), whereClause)

	if limit > 0 {
		query += fmt.Sprintf("\nLIMIT %d", limit)
	}

	return query
}

func queryGrantsToRole(ctx context.Context, cnf *Config, conn *sql.DB, db semantics.Ident, role semantics.Ident,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[Grant, error] {
	grantedTo := ObjTpRole
	if db != "" {
		grantedTo = ObjTpDatabaseRole
	}
	query := buildSQLQueryGrantsToRole(db, role, match, notMatch, cnf.Role, limit)
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
			var grantedRoleIsGruprManaged *bool
			var grantOption bool
			var grantedBy semantics.Ident
			if err = rows.Scan(&privilege, &createObjectType, &grantedOn, &name, &grantedRoleIsGruprManaged, &grantOption, &grantedBy); err != nil {
				yield(Grant{}, err)
				return
			}
			// NB: the caller decides which role to query, and therefore knows if the role starts with the prefix from Cnf
			g, err := newGrantToRole(privilege, createObjectType, grantedOn, name, grantedRoleIsGruprManaged, grantedTo, db, role, grantOption, grantedBy)
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

func QueryGrantsOfRoleToUsers(ctx context.Context, conn *sql.DB, role semantics.Ident) iter.Seq2[Grant, error] {
	return queryGrantsOfRole(ctx, conn, role, ObjTpUser)
}

func QueryGrantsOfRoleToRoles(ctx context.Context, conn *sql.DB, role semantics.Ident) iter.Seq2[Grant, error] {
	return queryGrantsOfRole(ctx, conn, role, ObjTpRole)
}

func queryGrantsOfRole(ctx context.Context, conn *sql.DB, role semantics.Ident, objTp ObjType) iter.Seq2[Grant, error] {
	// only used to query grants of product dtap roles, so, grantedRoleIsGruprManaged will be true
	return func(yield func(Grant, error) bool) {
		rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW GRANTS OF ROLE IDENTIFIER($$%v$$) ->>
SELECT
    "grantee_name" AS grantee_name
  , "granted_by" AS granted_by
FROM $1
WHERE "granted_to" = '%v'`, role, objTp))
		if err != nil {
			yield(Grant{}, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var granteeName semantics.Ident
			var grantedBy semantics.Ident
			if err = rows.Scan(&granteeName, &grantedBy); err != nil {
				yield(Grant{}, err)
				return
			}
			if !yield(newGrantOfRole(role, granteeName, grantedBy), nil) {
				return
			}
		}
		if err = rows.Err(); err != nil {
			yield(Grant{}, err)
			return
		}
	}
}

func QueryExternalGrantsOnDB(ctx context.Context, semCnf *semantics.Config, conn *sql.DB, db semantics.Ident) iter.Seq2[Grant, error] {
	return QueryExternalGrantsOnObject(ctx, semCnf, conn, ObjTpDatabase, db, semantics.Ident(""), semantics.Ident(""))
}

func QueryExternalGrantsOnSchema(ctx context.Context, semCnf *semantics.Config, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[Grant, error] {
	return QueryExternalGrantsOnObject(ctx, semCnf, conn, ObjTpSchema, db, schema, semantics.Ident(""))
}

func QueryExternalGrantsOnObject(ctx context.Context, semCnf *semantics.Config, conn *sql.DB, objTp ObjType,
	db semantics.Ident, schema semantics.Ident, obj semantics.Ident) iter.Seq2[Grant, error] {
	/*
		On a given object, query if there are any users (which are never managed by urupr) or (database) roles
		that do not start with semCnf.Prefix (and are thus assumed not to be managed by grupr either); that
		have privileges on the object. Return a single Grant object for each of them, with the ALL
		privilege. This Grant object can later be used to revoke all privileges on the object from this
		user or role.
	*/
	return func(yield func(Grant, error) bool) {
		rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW GRANTS ON IDENTIFIER($$%s$$) ->>
SELECT
    "granted_to" AS granted_to
  , "grantee_name" AS grantee_name
FROM $1
WHERE
     granted_to = 'USER'
  OR STARTSWITH(grantee_name, '%s')
GROUP BY
    granted_to
  , grantee_name`, objTp.FQN(db, schema, obj), semCnf.Prefix))
		if err != nil {
			yield(Grant{}, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var grantedTo string
			var granteeName string
			if err = rows.Scan(&grantedTo, &granteeName); err != nil {
				yield(Grant{}, err)
				return
			}
			if g, err := newExternalGrantOnObject(grantedTo, granteeName, objTp, db, schema, obj); err != nil {
				yield(Grant{}, err)
				return
			} else {
				if !yield(g, nil) {
					return
				}
			}
		}
		if err = rows.Err(); err != nil {
			yield(Grant{}, err)
			return
		}
	}
}

func DoGrants(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrants(ctx, cnf, conn, util.SeqAddNilError(grants), false)
}

func DoGrantsSkipErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrantsSkipErrors(ctx, cnf, conn, util.SeqAddNilError(grants), false)
}

func DoGrantsIndividually(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	for g := range grants {
		if err := runSQL(ctx, cnf, conn, g.buildSQLGrant(false)); err != nil {
			return err
		}
	}
	return nil
}

func DoRevokes(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrants(ctx, cnf, conn, util.SeqAddNilError(grants), true)
}

func DoRevokesSkipErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq[Grant]) error {
	return doGrantsSkipErrors(ctx, cnf, conn, util.SeqAddNilError(grants), true)
}

func DoRevokesExitOnInputErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq2[Grant, error]) error {
	return doGrants(ctx, cnf, conn, grants, true)
}

func doGrants(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq2[Grant, error], revoke bool) error {
	// Runs grant statements in batches
	buf := make([]string, cnf.StmtBatchSize)
	i := 0
	for g, inputErr := range grants {
		if inputErr != nil {
			return inputErr
		}
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

func doGrantsSkipErrors(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq2[Grant, error], revoke bool) error {
	for g, inputErr := range grants {
		if inputErr != nil {
			return inputErr
		}
		if err := runSQL(ctx, cnf, conn, g.buildSQLGrant(revoke)); err != nil && err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
	return nil
}
