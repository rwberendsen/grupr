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

func (g *Grupin) getProductDTAP(pID string, dtap string) (pd *ProductDTAP, isProd bool, ok bool) {
	if pd, ok = g.Prod[pID]; ok && pd.DTAP == dtap {
		isProd = true
		return 
	}
	if dtaps, ok = g.NonProd[pID]; ok {
		pd, ok = dtaps[dtap]
		return
	}
	return
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

func (g *Grupin) setDBRoleGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.db, pd ProductDTAP) error {
	// Loop over all granted grupr-managed database roles, and:
	// - store which ones we already have been granted.
	// - store which ones we should later revoke (when we have done a first granting loop over all products)
	grantedRoleStartsWithPrefix = true
	for grant, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.ReadRole.ID, true,
			map[GrantTemplate]struct{}{
				GrantTemplate{
					Privilege: PrvUsage,
					GrantedOn: ObjTpDatabaseRole,
					GrantedRoleStartsWithPrefix: &grantedRoleStartsWithPrefix,
				}: {}
			}, nil) {
		if err != nil { return err }
		grantedDBRole, err := newDatabaseRoleFromString(synCnf, cnf, grant.Database, grant.GrantedRole)
		if err != nil { return err }

		// Store if these database roles have already been granted:
		// - Read database role of pd product-level interface
		// - Read database roles of interfaces that pd consumes
		if grantedDBRole.ProductID == pd.ProductID {
			if grantedDBRole.InterfaceID != "" {
				pd.revokeGrantsToRead[grant] = struct{}{}
				continue
			}
			// grantedDBRole.InterfaceID == ""
			if dbObjs, ok := pd.Interface.aggAccountObjects.DBs[grant.Database]; ok {
				dbObjs.isDBRoleGrantedToProductRead = true
			} else if pd.Interface.ObjectMatchers.DisjointFromDB(grant.Database) {
				pd.revokeGrantsToRead[grant] = struct{}{}
			}
			continue
		}

		// grantedDBRole.ProductID != pd.ProductID
		if grantedDBRole.InterfaceID == "" {
			// we have no business with the product level interface of another product
			pd.revokeGrantsToRead[grant] = struct{}{}
			continue
		}

		// Check if granted database role belongs to an interface consumed by pd
		sourceDTAP, ok := pd.Consumes[syntax.InterfaceID{ID: grantedDBRole.InterfaceID, ProductID: grantedDBRole.ProductID,}]
		if !ok {
			// we do not consume that interface from that product
			pd.revokeGrantsToRead[grant] = struct{}{}
			continue
		}
		if sourceDTAP != grantedDBRole.DTAP {
			// we do consume that interface from that product, but not that dtap though
			pd.revokeGrantsToRead[grant] = struct{}{}
			continue
		}
		// sourceDTAP == grantedDBRole.DTAP

		// Okay, so the database role belongs to a known interface of another product-dtap that pd consumes, 
		sourcePD, _, ok := g.getProductDTAP(grantedDBRole, sourceDTAP)
		if !ok { panic("ProductDTAP not found, but known to be there") }
		sourceI, ok := sourcePD.Interfaces[grantedDBRole.InterfaceID]
		if !ok { panic("Interface not found, but known to be there") }

		// But, is it true that the database in which the granted role was created still has objects that belong to that interface?
		if sourceI.ObjectMatchers.DisjointFromDB(grantedDBRole.Database) {
			pd.revokeGrantsToRead[grant] = struct{}{}
			continue
		}

		// Okay, so indeed this grant is legitimate. If indeed we did find objects in this DB, then we have an AggDBObjs for it,
		// and we need to store in there that this grant has already been done, so that we don't make an unnecessary network request
		// granting it again. 
		if sourceAggDBObjs, ok := sourceI.aggAccountObjects.DBs[grantedDBRole.Database]; ok {
			sourceI.aggAccountObjects.DBs[grantedDBRole.Database] = sourceAggDBObjs.setConsumedByGranted(pd.ProductDTAPID)
		}
	}
}

func (g *Grupin) getTodoDBRoleGrants(doProd bool) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for _, pd := range g.Prod {
			pd.pushToDoDBRoleGrants(yield, doProd, func isProd(pd ProductDTAP) bool {
				_, is, ok := g.getProductDTAP(pd.ProductID, pd.DTAP)
				return ok && is
			})
		}
		if !doProd {
			for _, pd := range g.NonProd {
				pd.pushToDoDBRoleGrants(yield, doProd, func isProd(pd ProductDTAP) bool {
					_, is, ok := g.getProductDTAP(pd.ProductID, pd.DTAP)
					return ok && is
				})
			}
		}
	}
}

func (g *Grupin) grantProd(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.db) error {
	// The bulk of the grants are granting objects to roles, we do it concurrently per product-dtap
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for _, pd := range g.Prod {
		eg.Go(func() error { return pd.grant(ctx, synCnf, cnf, conn, g.productRoles, g.createDBRoleGrants, g.accountCache) })
	}
	err := eg.Wait()
	if err != nil { return err }
	// Now all necessary db roles have been created and they have been granted the necessary privileges

	// Next, find out which DB roles have been granted to which product roles, and which grants still to do / revoke
	// We do not do this concurrently, because this concerns relationships between product dtaps, no need to overcomplicate
	for _, pd := range g.Prod {
		if err := g.setDBRoleGrants(ctx, synCnf, cnf, conn, pd); err != nil { return err }
	}

	// Next, do the grants still to do.
	DoGrants(ctx, cnf, conn, g.getTodoDBROleGrants(true))
}

func (g *grupin) grantNonProd(ctx context.context, synCnf *syntax.Config, cnf *Config, conn *sql.db) error {
	// The bulk of the grants are granting objects to roles, we do it concurrently per product-dtap
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for _, dtaps := range g.NonProd {
		for _, pd := range dtaps {
			eg.Go(func() error { return pd.grant(ctx, synCnf, cnf, conn, g.productRoles, g.createDBRoleGrants, g.accountCache) })
		}
	}
	err := eg.Wait()
	if err != nil { return err }
	// Now all necessary db roles have been created and they have been granted the necessary privileges

	// Next, find out which DB roles have been granted to which product roles, and which grants still to do / revoke
	// We do not do this concurrently, because this concerns relationships between product dtaps, no need to overcomplicate
	for _, dtaps := range g.NonProd {
		for _, pd := range dtaps {
			if err := g.setDBRoleGrants(ctx, synCnf, cnf, conn, pd); err != nil { return err }
		}
	}

	// Next, do the grants still to do.
	DoGrants(ctx, cnf, conn, g.getTodoDBROleGrants(false))
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
			map[GrantTemplate]struct{}{
				GrantTemplate{
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
