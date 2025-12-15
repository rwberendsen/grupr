package snowflake

import (
	"context"
	"database/sql"

	"golang.org/x/sync/errgroup" 
	"github.com/rwberendsen/grupr/internal/semantics"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Products map[string]*Product
	Roles map[string]bool // false means no evidence from YAML (yet) that we need this role
	DatabaseRoles map[string]map[string]bool // false means no evidence from YAML (yet) that we need this role
	accountCache *accountCache
	// TODO: where we use map[string]bool but the bool has no meaning, use struct{} instead: more clearly meaningless
}

func NewGrupin(ctx context.Context, cnf *Config, conn *sql.DB, g semantics.Grupin) (Grupin, error) {
	r := Grupin{Products: map[string]*Product{}, accountCache: &accountCache{},}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductThreads)
	for k, v := range g.Products {
		r.Products[k] = newProduct(v)
		eg.Go(func() error { return r.Products[k].refresh(ctx, cnf, conn, r.AccountCache) }
	}
	err := eg.Wait()
	return r, err
}

func (g *Grupin) ManageReadAcces(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// first process grants, then revokes, to minimize downtime
	if err := g.grant(ctx, cnf, conn); err != nil { return err }
	if err := g.revoke(ctx, cnf, conn); err != nil { return err }
}

func (g *grupin) grant(ctx context.context, cnf *Config, conn *sql.db) error {
	if err := g.setDatabaseRoles(ctx, conn); err != nil { return err }
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

func (g *Grupin) setDatabaseRoles(ctx context.Context, conn *sql.DB) error {
	// query SHOW DATABASES IN ACCOUNT
	for db := range dbs {
		// query SHOW DATABASE ROLES IN DATABASE db
		for role {
			g.DatabaseRoles[db][role] = Existence{Exists: true,}
		}
	}
	for pid, p := range g.Products {
		for db := range p.AccountObjects() {
			if dbRoles, ok := g.DatabaseRoles[db]; ok {
				if _, ok = dbRoles['prefix_pid_r']; ok {
					dbRoles['prefix_pid_r'] = true
				}
			}
			g.DatabaseRoles[db]['prefix_pid_r'] = true
		}
		for iid, i := range p.Interfaces
			for db := range i.AccountObjects {
				if ... 
			}
		}
	}
	// in the end, we'll drop roles that existed, but were not matched by any product in the yaml; if nobody has been granted these roles
}


func (g Grupin) String() string {
	// TODO: consider stripping this silly yaml serializing from semantics and snowflake package; it's really the domain of the syntax package only
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
