package snowflake

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"iter"
	"maps"
	"strings"
)

type FutureGrant struct {
	Privileges        []PrivilegeComplete
	GrantedOn         ObjType
	GrantedIn         ObjType
	Database          string
	Schema            string
	GrantedTo         ObjType
	GrantedToDatabase string
	GrantedToRole     string
	GrantOption       bool // TODO: if we re-grant the same grant with a different grant option, does it get overwritten? Could be a way to correct such mishaps
}

func (g FutureGrant) buildSQLGrant(revoke bool) string {
	verb := `GRANT`
	preposition := `TO`
	if revoke {
		verb = `REVOKE`
		preposition = `FROM`
	}

	var granteeClause string
	switch g.GrantedTo {
	case ObjTypeRole:
		granteeClause = fmt.Sprintf(`%s ROLE %s`, preposition, quoteIdentifier(g.GrantedToRole))
	case ObjTypeDatabaseRole:
		granteeClause = fmt.Sprintf(`%s DATABASE ROLE %s.%s`, preposition, quoteIdentifier(g.GrantedToDatabase), quoteIdentifier(g.GrantedToRole))
	default:
		panic("Not implemented")
	}

	privilegeClause := strings.Join(g.Privileges, `, `)

	onClause := `ON FUTURE `
	inClause := `IN `
	switch g.GrantedOn {
	case ObjTpSchema:
		// Only supported IN DATABASE, only USAGE supported
		if g.GrantedIn != ObjTpDatabase || g.Privilege != PrvUsage {
			panic("Not implemented")
		}
		inClause += fmt.Sprintf(`%v %s`, g.GrantedIn, quoteIdentifier(g.Database))
	case ObjTpTable, ObjTpView:
		if g.Privilege != PrvSelect && g.Privilege != PrvReferences {
			panic("Not implemented yet")
		}
		switch g.GrantedIn {
		case ObjTpDatabase:
			inClause += fmt.Sprintf(`%v %s`, g.GrantedIn, quoteIdentifier(g.Database))
		case ObjTpSchema:
			inClause += fmt.Sprintf(`%v %s.%s`, g.GrantedIn, quoteIdentifier(g.Database), quoteIdentifier(g.Schema))
		default:
			panic("Not implemented")
		}
	default:
		panic("Not implemented")
	}

	onClause += fmt.Sprintf(`%vS`, g.GrantedOn)
	return fmt.Sprintf(`%v %s %s %s %s`, verb, privilegeClause, onClause, inClause, granteeClause)
}

func newFutureGrant(privilege string, createObjType string, grantedOn string, name string, grantedTo ObjType,
	grantedToDatabase string, grantedToRole string, grantOption bool) (FutureGrant, error) {
	g := FutureGrant{
		Privileges:        []PrivilegeComplete{ParsePrivilegeComplete(privilege, createObjType)},
		GrantedOn:         ParseObjType(grantedOn),
		GrantedTo:         grantedTo,
		GrantedToDatabase: grantedToDatabase,
		GrantedToRole:     grantedToRole,
		GrantOption:       grantOption,
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
	switch len(rec) {
	case 2:
		g.GrantedIn = ObjTpDatabase
		g.Database = rec[0]
	case 3:
		g.GrantedIn = ObjTpSchema
		g.Database = rec[0]
		g.Schema = rec[1]
	default:
		return g, fmt.Errorf("parsing name in future grant failed")
	}
	switch g.GrantedOn {
	case ObjTpSchema:
		g.Database = rec[0]
		g.Schema = rec[1]
	case ObjTpTable, ObjTpView:
		g.Database = rec[0]
		g.Schema = rec[1]
		g.Object = rec[2]
	default:
		return g, fmt.Errorf("unsupported granted_on object type for future grant")
	}
	return g, nil
}

func QueryFutureGrantsToRoleFiltered(ctx context.Context, conn *sql.DB, role string,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, "", role, match, notMatch, 0)
}

func QueryFutureGrantsToDBRoleFiltered(ctx context.Context, conn *sql.DB, db string, role string,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, db, role, match, notMatch, 0)
}

func QueryFutureGrantsToRole(ctx context.Context, conn *sql.DB, role string) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, "", role, nil, nil, 0)
}

func QueryFutureGrantsToDBRole(ctx context.Context, conn *sql.DB, db string, role string) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, db, role, nil, nil, 0)
}

func QueryFutureGrantsToRoleFilteredLimit(ctx context.Context, conn *sql.DB, role string,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, "", role, match, notMatch, limit)
}

func buildSQLQueryFutureGrants(db string, role string, match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) string {
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

	var sql string
	sql := fmt.Sprintf(`SHOW FUTURE GRANTS TO %sROLE IDENTIFIER('%s')
->> SELECT
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ') THEN 'CREATE'
    ELSE "privilege"
    END AS privilege
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ') THEN SUBSTR("privilege", 8)
    ELSE NULL
    END AS create_object_type
  , "grant_on"		AS granted_on
  , "name"		AS name
  , "grant_option"	AS grant_option
FROM $1%s`, dbClause, granteeName, whereClause)

	if limit > 0 {
		sql += fmt.Sprintf("\nLIMIT %d", limit)
	}

	return
}

func queryFutureGrantsToRole(ctx context.Context, conn *sql.DB, db string, role string,
	match map[GrantTemplate]struct{}, notMatch map[GrantTemplate]struct{}, limit int) iter.Seq2[FutureGrant, error] {
	// Note that both db and string will be quoted before going to Snowflake, so
	// if the names in Snowflake are upper case, present them here in upper case, too.
	grantedTo := ObjTpRole
	if db != "" {
		grantedTo = ObjTpDatabaseRole
	}
	sql := buildSQLQueryFutureGrants(db, role, match, notMatch, limit)
	return func(yield func(FutureGrant, error) bool) {
		rows, err := conn.QueryContext(ctx, sql, param)
		defer rows.Close()
		if err != nil {
			if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
				err = ErrObjectNotExistOrAuthorized
			}
			yield(FutureGrant{}, err)
			return
		}
		for rows.Next() {
			var privilege string
			var createObjectType string
			var grantedOn string
			var name string
			var grantOption bool
			if err = rows.Scan(&privilege, &createObjectType, &grantedOn, &name, &grantOption); err != nil {
				yield(FutureGrant{}, err)
				return
			}
			g, err := newFutureGrant(privilege, createObjectType, grantedOn, name, grantedTo, db, role, grantOption)
			if err != nil {
				yield(FutureGrant{}, err)
			}
			if !yield(g, nil) {
				return
			}
		}
		if err = rows.Err(); err != nil {
			yield(FutureGrant{}, err)
			return
		}
	}
}

func DoFutureGrants(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq2[FutureGrant, error]) error {
	return doFutureGrants(ctx, cnf, conn, grants, false)
}

func DoFutureRevokes(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq2[FutureGrant, error]) error {
	return doFutureGrants(ctx, cnf, conn, grants, true)
}

func doFutureGrants(ctx context.Context, cnf *Config, conn *sql.DB, grants iter.Seq2[FutureGrant, error], revoke bool) error {
	// Runs grant statements in batches
	buf := make([]string, cnf.StmtBatchSize)
	i := 0
	for g := range grants {
		if i == cnf.StmtBatchSize {
			if err := runMultipleSQL(ctx, cnf, conn, slices.Join(buf, ";"), i); err != nil {
				return err
			}
			i = 0
		}
		buf[i] := g.buildSQLGrant(revoke)
		i++
	}
	if i > 0 {
		if err := runMultipleSQL(ctx, cnf, conn, slices.Join(buf[0:i], ";"), i); err != nil {
			return err
		}
	}
	return nil
}
