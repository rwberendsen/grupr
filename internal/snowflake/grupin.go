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
	Roles map[ProductRole]struct{}
	DatabaseRoles map[string]map[DatabaseRole]struct{}
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
	if err := g.setRoles(); err != nil { return err }
	if err := g.setDatabaseRoles(ctx, conn); err != nil { return err }
	// first process grants, then revokes, to minimize downtime
	if err := g.grantRead(ctx, cnf, conn); err != nil { return err }
	if err := g.revokeRead(ctx, cnf, conn); err != nil { return err }
	if err := g.dropRoles(ctx, synCnf, cnf, conn); err != nil { return err }
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

func (g *Grupin) setRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	g.Roles = map[ProductRole]struct{}{}
	rows, err := conn.QueryContext(ctx, `SHOW TERSE ROLES LIKE ? ->> SELECT "name" FROM $1`, cnf.Prefix + "%")
	if err != nil { return err }
	for rows.Next() {
		var roleName string
		if err = rows.Scan(&roleName); err != nil { return err }
		if r, err := newProductRole(roleName); err != nil {
			return err
		} else {
			g.Roles[r] = struct{}{}
		}
	}
	if err = rows.Err(); err != nil { return err }
}

func (g *Grupin) setDatabaseRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	g.DatabaseRoles = map[string]map[DatabaseRole]struct{}{}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductThreads) // we are not handling products here, but still a sensible choice
	for db := range g.accountCache.getDBs() {
		g.DatabaseRoles[db.Name] = map[DatabaseRole]struct{}{}
		eg.Go(func() error { return queryDatabaseRoles(ctx, cnf, conn, g.DatabaseRoles[db.Name]) })
	}
	err := eg.Wait()
	return err
}

func (g *Grupin) dropRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	for role := g.Roles {
		if g.gSem. // WIP: just over the YAML, and check whether the (database) roles we found can possibly be matched by the YAML; if not; check if roles are granted to any (non-system) role and if not then drop
	}
}

func queryDatabaseRoles(ctx context.Context, cnf *Config, conn *sql.DB, m map[string]struct) error {
	rows, err := conn.QueryContext(ctx, `SHOW DATABASE ROLES IN DATABASE IDENTIFIER(?) ->> SELECT "name" FROM $1 WHERE "owner" = ? `, cnf.Prefix + "%", strings.ToUpper(cnf.Role))
	if err != nil { 
		if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
			return nil // perhaps DB was removed concurrently, just don't populate m
		}
		return err 
	}
	for rows.Next() {
		var roleName string
		if err = rows.Scan(&roleName); err != nil { return err }
		if r, err := newDatabaseRole(roleName); err != nil {
			return err
		} else {
			m[r] = struct{}{}
		}
	}
	if err = rows.Err(); err != nil { return err }
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
