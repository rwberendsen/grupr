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
	Prod map[string]*ProductDTAP // Do prod first, in its entirety
	NonProd map[string]map[string]*ProductDTAP // map[ProductID]map[DTAP]; rinse and repeat

	hasProdObjects bool
	hasProdAccessManaged bool
	hasNonProdObjects bool
	hasNonProdAccessManaged bool

	// Some fetch-one time reference data on objects that exist in Snowflake already
	productRoles map[ProductRole]struct{}
	createDBRoleGrants map[string]struct{}

	// The account cache, used to fetch objects by several concurrent threads, possibly from the same databases and schemas
	accountCache *accountCache
}

func NewGrupin(ctx context.Context, cnf *Config, conn *sql.DB, g semantics.Grupin) (*Grupin, error) {
	r := Grupin{
		Prod: map[string]*ProductDTAP{},
		NonProd: map[string]map[string]*ProductDTAP{},
		accountCache: &accountCache{},
	}

	for pID, pSem := range g.Products {
		var prodDTAP string
		if p.DTAPs.Prod != nil {
			prodDTAP = *pSem.DTAPs.Prod
			r.Prod[pID] = NewProductDTAP(pID, prodDTAP, pSem)
		}

		r.NonProd[pID] = map[string]*ProductDTAP{}
		for dtap := range p.DTAPs.NonProd {
			r.NonProd[pID][dtap] = NewProductDTAP(pID, dtap, pSem)
		}
	}

}

func (g *Grupin) SetObjectsProd(ctx context.Context, cnf *Context, conn *sql.DB) error {
	if g.hasProdObjects { return nil }
	// Calculating objects can be a time-consuming activity.
	// If you want to manage access, it may make sense to first fetch objects only for production environments
	// and then proceed to grant access for those production environments first.

	// In non production environments, there may be even more objects, and it can be more volatile as well,
	// requiring more retries or even re-runs to get things right. 
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for pID, pd := range g.Prod {
		eg.Go(func() error { return pd.refresh(ctx, cnf, conn, r.AccountCache) }
	}
	err := eg.Wait()
	if err == nil {
		g.hasProdObjects = true
	}
	return r, err
}

func (g *Grupin) SetObjectsNonProd(ctx context.Context, cnf *Context, conn *sql.DB) error {
	if g.hasNonProdObjects { return nil }
	// Calculating objects can be a time-consuming activity.
	// If you want to manage access, it may make sense to first fetch objects only for production environments
	// and then proceed to grant access for those production environments first.

	// In non production environments, there may be even more objects, and it can be more volatile as well,
	// requiring more retries or even re-runs to get things right. 
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for pID, dtaps := range g.NonProd {
		for _, pd := range dtaps {
			eg.Go(func() error { return pd.refresh(ctx, cnf, conn, r.AccountCache) }
		}
	}
	err := eg.Wait()
	if err == nil {
		g.hasNonProdObjects = true
	}
	return r, err
}

func (g *Grupin) ManageAccessProd(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	if g.hasProdAccessManaged { return nil }
	// first process grants, then revokes, to minimize downtime
	// first process write rights, then read rights, otherwise COPY GRANTS on GRANT OWNERSHIP statements
	//   may copy unnecessarily many grants
	// whether granting or revoking, first process FUTURE GRANTS, then usual grants; otherwise concurrently created
	//   objects may be missed out in a run.
	if err := g.setProductRoles(ctx, synCnf, cnf, conn); err != nil { return err }
	if err := g.setCreateDBRoleGrants(ctx, cnf, conn); err != nil { return err }
	if err := g.grantProd(ctx, cnf, conn); err != nil { return err }
	if err := g.revokeProd(ctx, cnf, conn); err != nil { return err }
	if err := g.dropDatabaseRoles(ctx, synCnf, cnf, conn); err != nil { return err }
	if err := g.dropProductRoles(ctx, synCnf, cnf, conn); err != nil { return err }
	g.hasProdAccessManaged = true
	return nil
}

func (g *Grupin) ManageAccessNonProd(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	if g.hasNonProdAccessManaged { return nil }
	if !g.hasProdAccesssManaged {
		return fmt.Errrof("need to have prod access managed first")
	}
	// first process grants, then revokes, to minimize downtime
	// first process write rights, then read rights, otherwise COPY GRANTS on GRANT OWNERSHIP statements
	//   may copy unnecessarily many grants
	// whether granting or revoking, first process FUTURE GRANTS, then usual grants; otherwise concurrently created
	//   objects may be missed out in a run.
	if err := g.grantNonProd(ctx, cnf, conn); err != nil { return err }
	if err := g.revokeNonProd(ctx, cnf, conn); err != nil { return err }
	g.hasNonProdAccessManaged = true
	return nil
}


func (g *grupin) grantProd(ctx context.context, synCnf *syntax.Config, cnf *Config, conn *sql.db) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for _, pd := range g.Prod {
		eg.Go(func() error { return pd.grant(ctx, synCnf, cnf, conn, g.productRoles, g.createDBRoleGrants, g.accountCache) })
	}
	for _, pd := range g.Prod {
		for iid := range pd.Consumes {
			// grant relevant database roles to product read role
		}
	}
}

func (g *grupin) grantNonProd(ctx context.context, synCnf *syntax.Config, cnf *Config, conn *sql.db) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for pID, dtaps := range g.NonProd {
		for _, pd := range dtaps {
			eg.Go(func() error { return pd.grant(ctx, synCnf, cnf, conn, g.productRoles, g.createDBRoleGrants, g.accountCache) })
		}
	}
	for pID, dtaps := range g.NonProd {
		for _, pd := range dtaps {
			for iid, dtap := range pd.Consumes {
				// grant relevant database roles to product read role
				// Note that dtap can refer to a NonProd dtap, but also a prod one
			}
		}
	}
}

func (g *grupin) revokeProd(ctx context.context, cnf *Config, conn *sql.db) error {
	for _, p := range g.Products {
		// revoke relevant database roles from product role
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for k, v := range g.Products {
		eg.Go(func() error { return v.revoke(ctx, cnf, conn) })
	}
	err := eg.Wait()
	return err 
}

func (g *grupin) revokeNonProd(ctx context.context, cnf *Config, conn *sql.db) error {
	for _, p := range g.Products {
		// revoke relevant database roles from product role
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for k, v := range g.Products {
		eg.Go(func() error { return v.revoke(ctx, cnf, conn) })
	}
	err := eg.Wait()
	return err 
}

func (g *Grupin) setProductRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	g.productRoles = map[ProductRole]struct{}{}
	rows, err := conn.QueryContext(ctx, `SHOW TERSE ROLES LIKE ? ->> SELECT "name" FROM $1`, cnf.Prefix + "%")
	if err != nil { return err }
	for rows.Next() {
		var roleName string
		if err = rows.Scan(&roleName); err != nil { return err }
		if r, err := newProductRoleFromString(synCnf, cnf, roleName); err != nil {
			return err
		} else {
			g.productRoles[r] = struct{}{}
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
	for r := range g.productRoles {
		if pSem, ok := g.gSem.Products[r.ProductID]; ok {
			if pSem.DTAPs.HasDTAP(r.DTAP) {
				continue // no need to drop this role
			}
		}
		if err := r.Drop(ctx, cnf, conn); err != nil { return err }
	}
}

func (g *Grupin) dropDatabaseRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for db, dbCache := range g.accountCache.getDBs() {
		for r := range dbCache.dbRoles {
			if pSem, ok := g.Sem.Products[r.ProductID]; ok {
				if pSem.DTAPs.HasDTAP(r.DTAP) {
					if r.InterfaceID == "" {
						if !pSem.ObjectMatchers.DisjointFromDB(db) {
							continue // this role is still needed
						}
					} else if iSem, ok := pSem.Interfaces[r.InterfaceID]; ok {
						if !iSem.ObjectMatchers.DisjointFromDB(db) {
							continue // this role is still needed
						}
					}
				}
			}
			if err := r.Drop(ctx, cnf, conn); err != nil { return err }
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
