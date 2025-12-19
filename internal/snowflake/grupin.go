package snowflake

import (
	"context"
	"database/sql"
	"errgroup"
	"strings"

	"golang.org/x/sync/errgroup" 
	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Products map[string]*Product
	ProductRoles map[ProductRole]struct{}
	DatabaseRoles map[DBKey]map[DatabaseRole]struct{}
	gSem semantics.Grupin
	accountCache *accountCache
	// TODO: where we use map[string]bool but the bool has no meaning, use struct{} instead: more clearly meaningless
}

func NewGrupin(ctx context.Context, cnf *Config, conn *sql.DB, g semantics.Grupin) (Grupin, error) {
	r := Grupin{Products: map[string]*Product{}, gSem: g, accountCache: &accountCache{},}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductThreads)
	for k, v := range g.Products {
		r.Products[k] = newProduct(v)
		eg.Go(func() error { return r.Products[k].refresh(ctx, cnf, conn, r.AccountCache) }
	}
	err := eg.Wait()
	return r, err
}

func (g *Grupin) ManageAccess(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	if err := g.setProductRoles(ctx, synCnf, cnf, conn); err != nil { return err }
	if err := g.setDatabaseRoles(ctx, conn); err != nil { return err }
	// first process grants, then revokes, to minimize downtime
	if err := g.grantRead(ctx, cnf, conn); err != nil { return err }
	if err := g.revokeRead(ctx, cnf, conn); err != nil { return err }
	if err := g.dropProductRoles(ctx, synCnf, cnf, conn); err != nil { return err }
}

func (g *grupin) grantRead(ctx context.context, cnf *Config, conn *sql.db) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductThreads)
	for k, v := range g.Products {
		eg.Go(func() error { return v.grant(ctx, cnf, conn, g.DatabaseRoles) })
	}
	for _, p := range g.Products {
		for iid, dtapMapping := range v.pSem.Consumes {
			// grant relevant database roles to product read role
		}
	}
}

func (g *grupin) revoke(ctx context.context, cnf *Config, conn *sql.db) error {
	for _, p := range g.Products {
		// revoke relevant database roles from product role
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductThreads)
	for k, v := range g.Products {
		eg.Go(func() error { return v.revoke(ctx, cnf, conn) })
	}
	err := eg.Wait()
	return err 
}

func (g *Grupin) setProductRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	g.ProductRoles = map[ProductRole]struct{}{}
	rows, err := conn.QueryContext(ctx, `SHOW TERSE ROLES LIKE ? ->> SELECT "name" FROM $1`, cnf.Prefix + "%")
	if err != nil { return err }
	for rows.Next() {
		var roleName string
		if err = rows.Scan(&roleName); err != nil { return err }
		if r, err := newProductRoleFromString(synCnf, cnf, roleName); err != nil {
			return err
		} else {
			g.ProductRoles[r] = struct{}{}
		}
	}
	if err = rows.Err(); err != nil { return err }
}

func (g *Grupin) setDatabaseRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	g.DatabaseRoles = map[DBKey]map[DatabaseRole]struct{}{}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductThreads) // we are not handling products here, but still a sensible choice
	for db := range g.accountCache.getDBs() {
		g.DatabaseRoles[db] = map[DatabaseRole]struct{}{}
		eg.Go(func() error { return queryDatabaseRoles(ctx, synCnf, cnf, conn, g.DatabaseRoles[db]) })
	}
	err := eg.Wait()
	return err
}

func (g *Grupin) dropProductRoles(ctx context.Context, conn *sql.DB) error {
	for r := range g.ProductRoles {
		if pSem, ok := g.gSem.Products[r.ProductID]; ok {
			if pSem.DTAPs.HasDTAP(r.DTAP) {
				continue // no need to drop this role
			}
		}
		if err := dropRole(ctx, conn, r.ID); err != nil { return err }
	}
}

func (g *Grupin) dropDatabaseRoles(ctx context.Context, conn *sql.DB) error {
	// WIP: just over the YAML, and check whether the
	//(database) roles we found can possibly be matched by the YAML; if not; check if
	//roles are granted to any (non-system) role and if not then drop
	for db, dbRoles := range g.DatabaseRoles {
		for r := range dbRoles {
			if pSem, ok := g.Sem.Products[r.ProductID]; ok {
				if pSem.DTAPs.HasDTAP(r.DTAP) {
					if r.InterfaceID == "" {
						if pSem.ObjectMatchers.MatchObjectsInDB(db.Name) {
							continue // this role is still needed
						}
						if err := dropDatabaseRole(ctx, conn, r.ID); err != nil { return err }
						continue
					}
					if iSem, ok := pSem.Interfaces[r.InterfaceID]; ok {
						if iSem.ObjectMatchers.MatchObjectsInDB(db.Name) {
							continue // this role is still needed
						}
						if err := dropDatabaseRole(ctx, conn, r.ID); err != nil { return err }
					}
				}
			}
		}
	}
}

func queryDatabaseRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, db DBKey, m map[string]struct) error {
	rows, err := conn.QueryContext(ctx, `SHOW DATABASE ROLES IN DATABASE IDENTIFIER(?) ->> SELECT "name" FROM $1 WHERE "owner" = ? `, db.Name, strings.ToUpper(cnf.Role))
	if err != nil { 
		if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
			return nil // perhaps DB was removed concurrently, just don't populate m
		}
		return err 
	}
	for rows.Next() {
		var roleName string
		if err = rows.Scan(&roleName); err != nil { return err }
		if r, err := newDatabaseRoleFromString(synCnf, cnf, roleName); err != nil {
			return err
		} else {
			m[r] = struct{}{}
		}
	}
	if err = rows.Err(); err != nil { return err }
	return nil
}

func dropRole(ctx context.Context, conn *sql.DB, role string) error {
	// If a role has CREATE, OWNERSHIP (ON FUTURE) privileges, do not drop it, or cnf.Role could end up owning objects.
	// Instead log a warning prompting administrators to GRANT OWNERSHIP to new owner and REVOKE any CREATE privileges.
	rows, err := conn.QueryContxt(ctx, `SHOW GRANTS ON ROLE IDENTIFIER(?)`, role)
	if err != nil { return err }
	for rows.Next() {
		// WIP
	}
	return nil
	// DROP ROLE IF EXISTS
	// TODO: note that DROP ROLE might time out if many grants would need to be transferred, in which case we can safely retry; except
	// in our case we don't want any transferring to happen.
}

func dropDatabaseRole(ctx context.Context, conn *sql.DB, dbName, dbRole string) error {
	// if the DB no longer exists, the role would not exist either anymore, we can ignore this error
	// WIP
	return nil
}

func (g Grupin) String() string {
	// TODO: consider stripping this silly yaml serializing from semantics and snowflake package; it's really the domain of the syntax package only
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
