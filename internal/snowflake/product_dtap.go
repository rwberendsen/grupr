package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"slices"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ProductDTAP struct {
	semantics.ProductDTAPID
	IsProd bool
	IsManual bool
	BlockCentralTeams bool
	*Interface
	Interfaces      map[string]*Interface
	Consumes        map[syntax.InterfaceID]string // value is source dtap
	ReadRole        ProductRole
	WriteRole       ProductRole
	GrantReadRoleToUsers  map[semantics.Ident]bool    // initially set to false, then to true if GRANTS are found in Snowflake
	GrantWriteRoleToUsers map[semantics.Ident]bool    // initially set to false, then to true if GRANTS are found in Snowflake
	DeployedBy      map[semantics.Ident]bool    // initially set to false, then to true if GRANTS are found in Snowflake
	ReadWarehouses  map[semantics.Ident][2]bool // initially set to false, then to true if GRANTS (USAGE, OPERATE) are found in Snowflake
	WriteWarehouses map[semantics.Ident][2]bool // initially set to false, then to true if GRANTS (USAGE, OPERATE) are found in Snowflake

	writeRoleGrantedToUserManagedRoles map[semantics.Ident]struct{}
	userManagedOwnersOfObjects         map[semantics.Ident]struct{}
	refreshCount                       int // how many times has this ProductDTAP been refreshed: populated with Snowflake objects
	matchedAccountObjects              map[semantics.ObjExpr]*matchedAccountObjs

	// These are only appended to, as we query different kinds of privileges granted to our product roles
	toRevoke []Grant

	// These are reset when refreshing objects, as these grants are on objects
	toRevokeObjects       []Grant
	toRevokeFutureObjects []FutureGrant
	toTransferOwnership   []Grant

	// Used for product dtap roles that exist in Snowflake but not in the YAML
	isZombie bool
}

func NewProductDTAP(pdID semantics.ProductDTAPID, isProd bool, pSem semantics.Product, userGroupMappings map[string]semantics.UserGroupMapping,
	svcs map[string]semantics.ServiceAccount, teams map[string]semantics.Team) *ProductDTAP {
	pd := &ProductDTAP{
		ProductDTAPID:         pdID,
		IsProd:                isProd,
		IsManual:              pSem.DTAPs.IsManual(pdID.DTAP),
		BlockCentralTeams:     pSem.BlockCentralTeams,
		Interface:             NewInterface(pdID.DTAP, pSem.InterfaceMetadata, userGroupMappings[pSem.UserGroupMappingID]),
		Interfaces:            map[string]*Interface{},
		Consumes:              map[syntax.InterfaceID]string{},
		GrantReadRoleToUsers:  map[semantics.Ident]bool{},
		GrantWriteRoleToUsers: map[semantics.Ident]bool{},
		ReadWarehouses:        map[semantics.Ident][2]bool{},
		WriteWarehouses:       map[semantics.Ident][2]bool{},
		matchedAccountObjects: map[semantics.ObjExpr]*matchedAccountObjs{},
	}

	for id, iSem := range pSem.Interfaces {
		pd.Interfaces[id] = NewInterface(pd.DTAP, iSem, userGroupMappings[pSem.UserGroupMappingID])
	}

	for iid, dtapMapping := range pSem.Consumes {
		if sourceDTAP, ok := dtapMapping[pd.DTAP]; ok {
			pd.Consumes[iid] = sourceDTAP
		}
		// else, pd.DTAP must have been in a list of non consuming dtaps for this consumption spec, and we do not consume from pd.DTAP
	}

	for k := range pd.Interface.ObjectMatchers {
		pd.matchedAccountObjects[k] = &matchedAccountObjs{}
	}

	// Set which service account users we should grant the write role to.
	for _, svc := range svcs {
		if dtapMapping, ok := svc.Deploys[pd.ProductID]; ok {
			if svcDTAP, ok := dtapMapping[pd.DTAP]; ok {
				pd.GrantWriteRoleTo[svc.Idents[svcDTAP]] = false // no GRANT found in Snowflake yet
			}
		}
	}

	// Set which personal users we should grant the read and write roles to.
	for _, team := range teams {
		if _, ok := team.WorkOn ... // WIP
	}

	return pd
}

func NewZombieProductDTAP(pdID semantics.ProductDTAPID) *ProductDTAP {
	return &ProductDTAP{
		ProductDTAPID: pdID,
		Interface:     &Interface{},
		isZombie:      true,
	}
}

func (pd *ProductDTAP) createProductRoles(ctx context.Context, semCnf *semantics.Config, cnf *Config,
	conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	// Read role
	pd.ReadRole = newProductRole(semCnf, pd.ProductID, pd.DTAP, ModeRead)
	if _, ok := productRoles[pd.ReadRole]; !ok {
		if err := pd.ReadRole.Create(ctx, cnf, conn); err != nil {
			return err
		}
	}

	// Write role, identical logic, maybe refactor
	pd.WriteRole = newProductRole(semCnf, pd.ProductID, pd.DTAP, ModeWrite)
	if _, ok := productRoles[pd.WriteRole]; !ok {
		if err := pd.WriteRole.Create(ctx, cnf, conn); err != nil {
			return err
		}
	}

	return nil
}

func (pd *ProductDTAP) setWriteRoleGrantedToUserManagedRoles(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB,
	productRoles map[ProductRole]struct{}) error {
	if _, ok := productRoles[pd.WriteRole]; !ok && cnf.DryRun {
		return nil
	}
	pd.writeRoleGrantedToUserManagedRoles = map[semantics.Ident]struct{}{}
	for g, err := range QueryGrantsOfRoleToRoles(ctx, conn, pd.WriteRole.ID) {
		if err != nil {
			return err
		}
		if strings.HasPrefix(string(g.GrantedToName), string(semCnf.Prefix)) {
			return fmt.Errorf("product dtap write role '%s' granted to other grupr managed role, please take action to correct")
		}
		if slices.Contains(cnf.SystemDefinedRoles, g.GrantedToName) {
			continue
		}
		pd.writeRoleGrantedToUserManagedRoles[g.GrantedToName] = struct{}{}
	}
	return nil
}

func (pd *ProductDTAP) setupProductRoles(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB,
	productRoles map[ProductRole]struct{}) error {
	// Create the product roles if necessary (read, write)
	if err := pd.createProductRoles(ctx, semCnf, cnf, conn, productRoles); err != nil {
		return err
	}

	// Check (once) which roles currently have been granted the write role;
	// we do this prior to claiming ownership of objects, so that the user managed roles who
	// had ownership before on these objects do not lose ownership (this could cause downtime in workloads)
	// Instead, sysadmins and developers are expected to use the product write role for their workloads.
	// When everything works, they can then revoke the product write role from their user managed role
	if err := pd.setWriteRoleGrantedToUserManagedRoles(ctx, semCnf, cnf, conn, productRoles); err != nil {
		return err
	}
	return nil
}

func (pd *ProductDTAP) grant(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool,
	userManagedOwners func(semantics.ProductDTAPID) map[semantics.Ident]struct{}, c *accountCache) error {
	// We manage grants on warehouses (once)
	// See product_dtap__warehouses.go for this method and its helper methods
	if err := pd.setWarehouseGrants(ctx, cnf, conn, productRoles); err != nil {
		return err
	}
	if err := DoGrantsSkipErrors(ctx, cnf, conn, pd.getToDoWarehouseGrants()); err != nil {
		return err
	}

	// We handle grants on objects like databases, schemas, tables, and
	// views, that may be created or dropped concurrently
	// We retry granting all privileges on such objects a number until we
	// encounter something else than an error about objects we expect to
	// exist having been dropped concurrently;
	// See product_dtap__objects.go for this method and its helper methods
	for {
		if err := pd.grant_(ctx, semCnf, cnf, conn, productRoles, grupinDisjointFromObject, userManagedOwners, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) getToDoDBRoleGrants(doProd bool, m map[semantics.ProductDTAPID]*ProductDTAP) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		pd.pushToDoDBRoleGrants(yield, doProd, m)
	}
}

func (pd *ProductDTAP) pushToDoDBRoleGrants(yield func(Grant) bool, doProd bool, m map[semantics.ProductDTAPID]*ProductDTAP) bool {
	// First grant database roles of product-level interface role to product read role
	for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
		if doProd == pd.IsProd {
			for _, pr := range [2]ProductRole{pd.ReadRole, pd.WriteRole} {
				if !dbObjs.isReadDBRoleGrantedToProductRole[pr.Mode.getIdx()] {
					if !yield(Grant{
						Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
						GrantedOn:     ObjTpDatabaseRole,
						Database:      db,
						GrantedRole:   dbObjs.readDBRole.Name,
						GrantedTo:     ObjTpRole,
						GrantedToName: pr.ID,
					}) {
						return false
					}
				}
			}
		}
	}
	// Next, grant database roles of interfaces to consumers (prod / non-prod)
	for _, i := range pd.Interfaces {
		if !i.pushToDoDBRoleGrants(yield, doProd, m) {
			return false
		}
	}
	return true
}

func (pd *ProductDTAP) pushToDoProductRoleGrants(yield func(Grant) bool) bool {
	for svc, alreadyGranted := range pd.DeployedBy {
		if !alreadyGranted {
			if !yield(Grant{
				Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
				GrantedOn:     ObjTpRole,
				GrantedRole:   pd.WriteRole.ID,
				GrantedTo:     ObjTpUser,
				GrantedToName: svc,
			}) {
				return false
			}
		}
	}
	return true
}

func (pd *ProductDTAP) setGrantedUsers(ctx context.Context, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	if _, ok := productRoles[pd.WriteRole]; !ok && cnf.DryRun {
		return nil
	}
	for grant, err := range QueryGrantsOfRoleToUsers(ctx, conn, pd.WriteRole.ID) {
		if err != nil {
			return err
		}
		if _, ok := pd.DeployedBy[grant.GrantedToName]; ok {
			pd.DeployedBy[grant.GrantedToName] = true
		} else {
			pd.toRevoke = append(pd.toRevoke, grant)
		}
	}
	return nil
}

func (pd *ProductDTAP) revokeGrantFromProductRole(g Grant) {
	pd.toRevoke = append(pd.toRevoke, g)
}

func (pd *ProductDTAP) revoke(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool,
	userManagedOwners func(semantics.ProductDTAPID) map[semantics.Ident]struct{}, c *accountCache) error {
	// We skip does-not-exist errors here, because:
	// - Users that do not exist are already revoked
	// - Warehouses that do not exist are already revoked
	// - DB roles that do not exist: it won't help refreshing just this product to refresh this info: a program re-run would be needed
	if err := DoRevokesSkipErrors(ctx, cnf, conn, slices.Values(pd.toRevoke)); err != nil {
		return err
	}

	// Now we handle revoking of privileges on objects that may be concurrently dropped.
	// If during revoking of privileges on objects to interface database
	// roles we get ErrObjectNotExistOrAuthorized, we can refresh the
	// product and then first grant again, and then revoke;
	//
	// In this case, too, it means some objects were dropped, and thus any privileges
	// on them would have been dropped as well, rendering our job already done.
	// So why then refresh? Some reasons:
	// - There can be many objects, thousands. If whole schemas or even databases were dropped,
	//   concurrently, it could mean thousands of queries done in vain, taking a lot of time.
	// - Because there can be many objects, we do multiple statements
	//   per network call. But if we get an error in those, the batch is only partially
	//   applied. Figuring out which statement caused the error, removing
	//   it from the batch, and retrying  could be a repetitive exercise, although it
	//   may be quicker than refreshing the whole product.
	// - But, finally, objects being dropped concurrently signals activity in this data
	//   product while Grupr was running. This could mean also some objects were added that
	//   we would need to grant now. Rather than ignoring that signal and wait for the
	//   next entire Grupr run, it can save our DBAs some time if we act immediately and
	//   refresh just this single data product-dtap.
	err := pd.revoke_(ctx, cnf, conn)
	for err == ErrObjectNotExistOrAuthorized {
		err = pd.grant_(ctx, semCnf, cnf, conn, productRoles, grupinDisjointFromObject, userManagedOwners, c)
		if err != nil {
			continue
		}
		err = pd.revoke_(ctx, cnf, conn)
	}
	return err
}

func (pd *ProductDTAP) dropProductRolesIfZombie(ctx context.Context, cnf *Config, conn *sql.DB) error {
	if !pd.isZombie {
		return nil
	}
	if len(pd.toTransferOwnership) > 0 {
		log.Printf("WARN: product '%s', dtap '%s', has ownership of objects, not dropping product roles", pd.ProductID, pd.DTAP)
		return nil
	}
	if err := pd.ReadRole.Drop(ctx, cnf, conn); err != nil {
		return err
	}
	return pd.WriteRole.Drop(ctx, cnf, conn)
}
