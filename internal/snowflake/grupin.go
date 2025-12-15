package snowflake

import (
	"context"
	"database/sql"

	"golang.org/x/sync/errgroup" 
	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Products map[string]*Product
	Roles map[string]struct
	DatabaseRoles map[string]map[string]struct
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
		eg.Go(func() error { return v.grant(ctx, cnf, conn, g.DatabaseRoles) }
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
		eg.Go(func() error { return v.revoke(ctx, cnf, conn) }
	}
}

func

func (g *Grupin) setDatabaseRoles(ctx context.Context, conn *sql.DB) error {
	// query SHOW DATABASES IN ACCOUNT
	for db := range dbs {
		// query SHOW DATABASE ROLES IN DATABASE db
		// if db does not exist anymore, continue with next one
		for role {
			g.DatabaseRoles[db][role] = Existence{Exists: true,}
		}
	}
	for pid, p := range g.Products {
	}
}

func (g *Grupin) dropRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	for role := g.Roles {
		dtap, pID, mode := get
		if g.gSem.
	}
}


func (g Grupin) String() string {
	// TODO: consider stripping this silly yaml serializing from semantics and snowflake package; it's really the domain of the syntax package only
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
