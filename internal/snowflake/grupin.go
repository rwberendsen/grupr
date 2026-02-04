package snowflake

import (
	"context"
	"database/sql"
	"iter"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	ProductDTAPs      map[semantics.ProductDTAPID]*ProductDTAP
	UserGroupMappings map[string]semantics.UserGroupMapping

	// Some fetch-one time reference data on objects that exist in Snowflake already
	productRoles       map[ProductRole]struct{}
	createDBRoleGrants map[string]struct{}

	// The account cache, used to fetch objects by several concurrent threads, possibly from the same databases and schemas
	accountCache *accountCache
}

func NewGrupin(ctx context.Context, cnf *Config, conn *sql.DB, g semantics.Grupin) *Grupin {
	r := &Grupin{
		ProductDTAPs:      map[semantics.ProductDTAPID]*ProductDTAP,
		UserGroupMappings: g.UserGroupMappings,
		accountCache:      &accountCache{},
	}

	for pID, pSem := range g.Products {
		for dtap, isProd := range pSem.DTAPs.All() {
			pdID := semantics.ProductDTAPID{ProductID: pID, DTAP: dtap}
			pd.ProductDTAPs[pdID] = NewProductDTAP(pdID, isProd, pSem, r.UserGroupMappings)
		}
	}
	return r
}

func (g *Grupin) setObjects(ctx context.Context, cnf *Config, conn *sql.DB, doProd bool) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for _, pd := range g.ProductDTAPs {
		if doProd == pd.IsProd {
			eg.Go(func() error { return pd.refresh(ctx, cnf, conn, r.AccountCache) })
		}
	}
	return eg.Wait()
}

func (g *Grupin) SetObjects(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// Calculating objects can be a time-consuming activity.
	// Since we care most about production, generally, we'll do it first.
	if err := g.setObjects(ctx, cnf, conn, true); err != nil {
		return err
	}
	if err := g.setObjects(ctx, cnf, conn, false); err != nil {
		return err
	}
	return nil
}

func (g *Grupin) manageAccess(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, doProd bool) error {
	// First process grants, then revokes, to minimize downtime
	// First process write rights, then read rights, otherwise COPY GRANTS on GRANT OWNERSHIP statements
	//   may copy unnecessarily many grants
	// Whether granting or revoking, first process FUTURE GRANTS, then usual grants; otherwise concurrently created
	//   objects may be missed out in a run.
	if err := g.grant(ctx, synCnf, cnf, conn, doProd); err != nil {
		return err
	}
	if err := g.revoke(ctx, synCnf, cnf, conn, doProd); err != nil {
		return err
	}
	if err := g.dropDatabaseRoles(ctx, synCnf, cnf, conn, doProd); err != nil {
		return err
	}
	if err := g.dropProductRoles(ctx, synCnf, cnf, conn, doProd); err != nil {
		return err
	}
	return nil
}

func (g *Grupin) ManageAccess(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	// Some global information we need to start managing access
	if err := g.setProductRoles(ctx, synCnf, cnf, conn); err != nil {
		return err
	}
	if err := g.setCreateDBRoleGrants(ctx, cnf, conn); err != nil {
		return err
	}

	// First complete production
	if err := g.manageAccess(ctx, synCnf, cnf, conn, true); err != nil {
		return err
	}
	if err := g.manageAccess(ctx, synCnf, cnf, conn, false); err != nil {
		return err
	}
	return nil
}

func (g *Grupin) setDBRoleGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pd ProductDTAP) error {
	// Loop over all granted grupr-managed database roles, and:
	// - store which ones we already have been granted.
	// - store which ones we should later revoke (when we have done a first granting loop over all products)
	for grant, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.ReadRole.ID, true, cnf.ProductRolePrivileges[ModeRead], nil) {
		if err != nil {
			return err
		}
		grantedDBRole, err := newDatabaseRoleFromString(synCnf, cnf, grant.Database, grant.GrantedRole)
		if err != nil {
			return err
		}

		// Store if these database roles have already been granted:
		// - Read database role of pd product-level interface
		// - Read database roles of interfaces that pd consumes
		if grantedDBRole.ProductID == pd.ProductID {
			if grantedDBRole.InterfaceID != "" {
				pd.revokeGrantFromRead(grant)
				continue
			}
			// grantedDBRole.InterfaceID == ""
			if dbObjs, ok := pd.Interface.aggAccountObjects.DBs[grant.Database]; ok {
				dbObjs.isDBRoleGrantedToProductRead = true
			} else if pd.Interface.ObjectMatchers.DisjointFromDB(grant.Database) {
				pd.revokeGrantFromRead(grant)
			}
			continue // leave this grant be, it is correct, even if it is unexpected that it exists
		}

		// grantedDBRole.ProductID != pd.ProductID
		if grantedDBRole.InterfaceID == "" {
			// we have no business with the product level interface of another product
			pd.revokeGrantFromRead(grant)
			continue
		}

		// Check if granted database role belongs to an interface consumed by pd
		sourceDTAP, ok := pd.Consumes[syntax.InterfaceID{ID: grantedDBRole.InterfaceID, ProductID: grantedDBRole.ProductID}]
		if !ok {
			// we do not consume that interface from that product
			pd.revokeGrantFromRead(grant)
			continue
		}
		if sourceDTAP != grantedDBRole.DTAP {
			// we do consume that interface from that product, but not that dtap though
			pd.revokeGrantFromRead(grant)
			continue
		}
		// sourceDTAP == grantedDBRole.DTAP

		// Okay, so the database role belongs to a known interface of another product-dtap that pd consumes,
		sourcePD := g.ProductDTAPs[semantics.ProductDTAPID{ProductID: grantedDBRole.ProductID, DTAP: sourceDTAP}]
		sourceI := sourcePD.Interfaces[grantedDBRole.InterfaceID]

		// But, is it true that the database in which the granted role was created still has objects that belong to that interface?
		if sourceI.ObjectMatchers.DisjointFromDB(grantedDBRole.Database) {
			pd.revokeGrantFromRead(grant)
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

func (g *Grupin) doToDoDBRoleGrants(ctx context.Context, cnf *Config, conn *sql.DB, doProd bool) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for _, pd := range g.ProductDTAPs {
		// Even if doProd == false, we still have to process also production product-dtaps, as their interfaces may be consumed by non-prod product-dtaps
		// So if doProd == false, we want to process all product-dtaps; otherwise, only production ones.
		if doProd == false || pd.isProd {
			eg.Go(func() error {
				return DoGrantsSkipErrors(ctx, cnf, conn, pd.getToDoDBRoleGrants(doProd, g.ProductDTAPs), false)
			})
			// Note that at this stage when we are touching all products, we just want to ignore obj not exist errors and move on
			// no point refreshing all products, we might as well re-run the whole program
		}
	}
	return eg.Wait()
}

func (g *Grupin) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, doProd bool) error {
	// The bulk of the grants are granting objects to roles, we do it concurrently per product-dtap
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for _, pd := range g.ProductDTAPs {
		if doProd == pd.isProd {
			eg.Go(func() error {
				return pd.grant(ctx, synCnf, cnf, conn, g.productRoles, g.createDBRoleGrants, g.accountCache)
			})
		}
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	// Now all necessary db roles have been created and they have been granted the necessary privileges

	// Next, find out which DB roles have been granted to which product roles, and which grants still to do / revoke
	// We do not do this concurrently, because this concerns relationships between product dtaps, no need to overcomplicate
	for _, pd := range g.ProductDTAPs {
		if doProd == pd.isProd {
			if err := g.setDBRoleGrants(ctx, synCnf, cnf, conn, pd); err != nil {
				return err
			}
		}
	}

	// Next, do the grants still to do.
	return g.doToDoDBRoleGrants(ctx, cnf, conn, doProd)
}

func (g *Grupin) revoke(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, doProd bool) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.MaxProductDTAPThreads)
	for _, pd := range g.ProdDTAPs {
		if doProd == pd.IsProd {
			eg.Go(func() error {
				return pd.revoke(ctx, synCnf, cnf, conn, g.productRoles, g.createDBRoleGrants, g.accountCache)
			})
		}
	}
	return eg.Wait()
}

func (g *Grupin) setProductRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB) error {
	g.productRoles = map[ProductRole]struct{}{}
	rows, err := conn.QueryContext(ctx, `SHOW TERSE ROLES LIKE ? ->> SELECT "name" FROM $1`, cnf.Prefix+"%")
	if err != nil {
		return err
	}
	for rows.Next() {
		var roleName string
		if err = rows.Scan(&roleName); err != nil {
			return err
		}
		if r, err := newProductRoleFromString(synCnf, cnf, roleName); err != nil {
			return err
		} else {
			g.productRoles[r] = struct{}{}
		}
	}
	return rows.Err()
}

func (g *Grupin) setCreateDBRoleGrants(ctx context.Context, cnf *Config, conn *sql.DB) error {
	g.createDBRoleGrants = map[string]struct{}{}
	for grant, err := range QueryGrantsToRoleFiltered(ctx, conn, cnf.Role,
		map[GrantTemplate]struct{}{
			GrantTemplate{
				Privilege:        PrvCreate,
				CreateObjectType: ObjTpDatabaseRole,
				GrantedOn:        ObjTpDatabase,
			}: {},
		},
		nil) {
		if err != nil {
			return err
		}
		g.createDBRoleGrants[grant.Database] = struct{}{}
	}
	return nil
}

func (g *Grupin) dropProductRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for r := range g.productRoles {
		if _, ok := g.ProductDTAPs[semantics.ProductDTAPID{ProductID: r.ProductID, DTAP: r.DTAP}]; ok {
			continue // no need to drop this role
		}
		if err := r.Drop(ctx, cnf, conn); err != nil {
			return err
		}
	}
	return nil
}

func (g *Grupin) dropDatabaseRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for db, dbCache := range g.accountCache.getDBs() {
		for r := range dbCache.dbRoles {
			if pd, ok := g.ProductDTAPs[semantics.ProductDTAPID{ProductID: r.ProductID, DTAP: r.DTAP}]; ok {
				if r.InterfaceID == "" {
					if !pd.Interface.ObjectMatchers.DisjointFromDB(db) {
						continue // this role is still needed
					}
				} else if i, ok := pd.Interfaces[r.InterfaceID]; ok {
					if !i.ObjectMatchers.DisjointFromDB(db) {
						continue // this role is still needed
					}
				}
			}
			if err := r.Drop(ctx, cnf, conn); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Grupin) GetObjCountsRows() iter.Seq[ObjCountsRow] {
	return func(yield func(ObjCountsRow) bool) {
		for pdID, pd := range g.ProductDTAPs {
			if !pd.pushObjectCounts(yield, pdID) {
				return
			}
		}
	}
}
