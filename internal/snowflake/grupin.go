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
	createDBRoleGrants map[string]struct{}
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
	// first process grants, then revokes, to minimize downtime
	// first process write rights, then read rights, otherwise COPY GRANTS on GRANT OWNERSHIP statements
	//   may copy unnecessarily many grants
	// whether granting or revoking, first process FUTURE GRANTS, then usual grants; otherwise concurrently created
	//   objects may be missed out in a run.
	if err := g.setProductRoles(ctx, synCnf, cnf, conn); err != nil { return err }
	if err := g.setCreateDBRoleGrants(ctx, cnf, conn); err != nil { return err }
	if err := g.grant(ctx, cnf, conn); err != nil { return err }
	if err := g.revoke(ctx, cnf, conn); err != nil { return err }
	if err := g.dropProductRoles(ctx, synCnf, cnf, conn); err != nil { return err }
}

func (g *grupin) grant(ctx context.context, synCnf *syntax.Config, cnf *Config, conn *sql.db) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductThreads)
	for k, v := range g.Products {
		eg.Go(func() error { return v.grant(ctx, synCnf, cnf, conn, g.ProductRoles, g.CreateDBRoleGrants) })
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

func (g *Grupin) setCreateDBRoleGrants(ctx context.Context, cnf *Config, conn *sql.DB) error {
	g.createDBRoleGrants = map[string]struct{}{}
	for grant, err := range QueryGrantsToRoleFiltered(ctx, conn, cnf.Role,
			map[GrantToRole]struct{}{
				GrantToRole{
					Privilege: PrvCreate,
					CreateObjectType: ObjTpDatabaseRole,
					GrantedOn: ObjTpDatabase,
				}: {}
			},
			nil) {
		if err != nil { return err }
		g.createDBRoleGrants[grant.Database] = struct{}{}
	}
}

func (g *Grupin) dropProductRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for r := range g.ProductRoles {
		if pSem, ok := g.gSem.Products[r.ProductID]; ok {
			if pSem.DTAPs.HasDTAP(r.DTAP) {
				continue // no need to drop this role
			}
		}
		if err := r.Drop(ctx, cnf, conn, r.ID); err != nil { return err }
	}
}

func (g *Grupin) dropDatabaseRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for db, dbCache := range g.accountCache.getDBs() {
		for r := range dbCache.dbRoles {
			if pSem, ok := g.Sem.Products[r.ProductID]; ok {
				if pSem.DTAPs.HasDTAP(r.DTAP) {
					if r.InterfaceID == "" {
						if pSem.ObjectMatchers.MatchObjectsInDB(db.Name) {
							continue // this role is still needed
						}
					} else if iSem, ok := pSem.Interfaces[r.InterfaceID]; ok {
						if iSem.ObjectMatchers.MatchObjectsInDB(db.Name) {
							continue // this role is still needed
						}
					}
				}
			}
			if err := r.Drop(ctx, conn); err != nil { return err }
		}
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
