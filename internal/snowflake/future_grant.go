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

type FutureGrant struct {
	// NOTE that this struct could be used to represent a GRANT ... ON ALL <object_type_plural> IN ... grant as well.
	// (Only syntactical difference between the statements is ALL <-> FUTURE)
	Privilege 		Privilege
	CreateObjectType	ObjType
	GrantedOn 		ObjType
	GrantedIn		ObjType 
	Database		string
	Schema			string
	GrantedTo		ObjType
	GrantedToDatabase	string
	GrantedToRole		string
	GrantOption		bool // TODO: if we re-grant the same grant with a different grant option, does it get overwritten? Could be a way to correct such mishaps
}

func (g FutureGrant) buildSQLGrant() string {
	if g.Privilege == PrvCreate {
		panic("Granting CREATE not implemented yet")
	}

	var toClause string
	switch g.GrantedTo {
		case ObjTypeRole:
			toClause = fmt.Sprintf(`TO ROLE %s`, quoteIdentifier(g.GrantedToRole))
		case ObjTypeDatabaseRole:
			toClause = fmt.Sprintf(`TO DATABASE ROLE %s.%s`, quoteIdentifier(g.GrantedToDatabase), quoteIdentifier(g.GrantedToRole))
		default:
			panic("Not implemented")
	}

	onClause := `ON FUTURE `
	inClause := `IN `
	switch g.GrantedOn {
	case ObjTpSchema:
		// Only supported IN DATABASE, only USAGE supported
		if g.GrantedIn != ObjTpDatabase || g.Privilege != PrvUsage {
			panic("Not implemented")
		}
		inClause += fmt.Sprintf(`%v %s`, g.GrantedIn, quoteIdentifier(g.Database))
	}
	case ObjTpTable || ObjTpView: 
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
	return fmt.Sprintf(`GRANT %s %s %s %s`, g.Privilege, onClause, inClause, toClause)
}

func newFutureGrant(privilege string, createObjType string, grantedOn string, name string, grantedTo ObjType,
		grantedToDatabase string, grantedToRole string, grantOption bool) (FutureGrant, error) {
	g := FutureGrant{
		Privilege: ParsePrivilege(privilege),
		CreateObjectType: ParseObjType(createObjType),
		GrantedOn: ParseObjType(grantedOn),
		GrantedTo: grantedTo,
		GrantedToDatabase: grantedToDatabase,
		GrantedToRole: grantedToRole,
		GrantOption: grantOption,
	}
	r := csv.NewReader(strings.NewReader(name) // handles quoted fields as they appear in name
	r.Comma = '.'
	rec, err := r.Read()
	if err != nil { return g, err }
	_, err = r.Read(); err != io.EOF { return g, err } // more than one record
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
		match map[Grant]struct{}, notMatch map[Grant]struct{}) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, "", role, match, notMatch, 0)
}

func QueryFutureGrantsToDBRoleFiltered(ctx context.Context, conn *sql.DB, db string, role string,
		match map[Grant]struct{}, notMatch map[Grant]struct{}) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, db, role, match, notMatch, 0)
}

func QueryFutureGrantsToRole(ctx context.Context, conn *sql.DB, role string) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, "", role, nil, nil, 0)
}

func QueryFutureGrantsToDBRole(ctx context.Context, conn *sql.DB, db string, role string) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, db, role, nil, nil, 0)
}

func QueryFutureGrantsToRoleFilteredLimit(ctx context.Context, conn *sql.DB, role string,
		match map[Grant]struct{}, notMatch map[Grant]struct{}, limit int) iter.Seq2[FutureGrant, error] {
	return queryFutureGrantsToRole(ctx, conn, "", role, match, notMatch, limit)
}

func (g FutureGrant) buildSQLFilter(g Grant) (string, int) {
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
	clauses = []string{} for g := range grants {
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

func buildSQLQueryFutureGrants(db string, role string, match map[Grant]struct{}, notMatch map[Grant]struct{}, limit int) string {
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
		match map[Grant]struct{}, notMatch map[Grant]struct{}, limit int) iter.Seq2[FutureGrant, error] {
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
				yield(FutureGrant{}, err}
			}
			if !yield(g, nil)
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
	buf := [cnf.StmtBatchSize]string{}
	i := 0
	for g, err := range grants {
		if err != nil { return err }
		buf[i] := g.buildSQL()
	}
}
