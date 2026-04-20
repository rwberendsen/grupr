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
	"github.com/rwberendsen/grupr/internal/util"
)

/*
In product_dtap__objects.go, we have ProductDTAP methods that deal mostly with (privileges on) objects
*/

func (pd *ProductDTAP) refresh(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	if err := pd.refresh_(ctx, semCnf, cnf, conn, c); err != nil {
		return err
	}
	pd.recalcObjects() // will reset all accountObjs
	// Reset other properties of pd that depend on which objects where matched
	pd.toRevokeFutureObjects = []FutureGrant{}
	pd.toRevokeObjects = []Grant{}
	pd.toTransferOwnership = []Grant{}
	return nil
}

func (pd *ProductDTAP) refresh_(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for {
		pd.refreshCount += 1
		if pd.refreshCount > cnf.MaxProductDTAPRefreshes {
			return fmt.Errorf("Max product refresh count reached")
		}
		util.SleepContext(ctx, 1<<pd.refreshCount-1) // exponential backoff
		if err := pd.refreshObjExprs(ctx, semCnf, cnf, conn, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) refreshObjExprs(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for e, om := range pd.ObjectMatchers {
		if err := c.match(ctx, semCnf, cnf, conn, om, pd.matchedAccountObjects[e]); err != nil {
			return err
		}
	}
	return nil
}

func (pd *ProductDTAP) recalcObjects() {
	pd.Interface.recalcObjectsFromMatched(pd.matchedAccountObjects)
	for _, v := range pd.Interfaces {
		v.recalcObjects(pd.accountObjects)
		v.aggregate() // this will free memory held by AccountObjs by ObjExpr
	}
	pd.Interface.aggregate() // we needed to hold on to AccountObjs by ObjExpr until we derived all interface objects
}

func (pd *ProductDTAP) setFutureGrantsToWriteRole(ctx context.Context, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	if _, ok := productRoles[pd.WriteRole]; !ok && cnf.DryRun {
		return nil
	}
	for g, err := range QueryFutureGrantsToRoleFiltered(ctx, conn, pd.WriteRole.ID, map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
			GrantedOn:         ObjTpSchema,
		}: {},
		// Ignoring other grants, including future ownership grants. It would be quite annoying if such grants
		// were present, they would interfere with grupr (basically grupr would correct them if they grant
		// ownership to any other role than the product dtap role). But, grupr does not use future ownership
		// grants itself. Instead, the idea is that sysadmins would arrange for any service account that
		// deploys objects in this product dtap to assume the product role; when doing so, ownership is
		// already automatic.

		// WIP: correct future grants on tables like truncate, insert, update, delete, etc; these would
		// be made redundant by ownership. In fact, that does mean we do manage these grants, in a way,
		// and yes, we would revoke them if we found them.
	}, nil) {
		if err != nil {
			return err
		}

		switch g.GrantedIn {
		case ObjTpDatabase:
			switch g.GrantedOn {
			case ObjTpSchema:
				// Should we have this grant?
				if pd.Interface.ObjectMatchers.MatchAllSchemasInDB(g.Database) {
					// If yes, then, if we also have matched the object, mark on it that privilege on future objects was already granted
					if dbObjs, ok := pd.Interface.aggAccountObjects.DBs[g.Database]; ok {
						dbObjs.setFutureGrantTo(ModeWrite, g)
					}
				} else {
					// if not, then revoke this future grant
					//
					// Note that when we refreshed, we reset toRevokeFutureObjects to the empty slice
					pd.toRevokeFutureObjects = append(pd.toRevokeFutureObjects, g)
				}
			}
			// Ignore this grant, it's not in grupr its scope (unmanaged grant)
		}
		// Ignore; unmanaged grant
	}
	return nil
}

func (pd *ProductDTAP) setGrantsToWriteRole(ctx context.Context, cnf *Config, conn *sql.DB,
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool, productRoles map[ProductRole]struct{}) error {
	if _, ok := productRoles[pd.WriteRole]; !ok && cnf.DryRun {
		return nil
	}
	for g, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.WriteRole.ID, cnf.ObjectPrivileges, nil) {
		if err != nil {
			return err
		}
		switch pc := g.Privileges[0]; pc.Privilege {
		case PrvCreate:
			switch pc.CreateObjectType {
			case ObjTpTable, ObjTpView:
				if !pd.Interface.ObjectMatchers.DisjointFromSchema(g.Database, g.Schema) {
					if dbObjs, ok := pd.Interface.aggAccountObjects.DBs[g.Database]; ok {
						if schemaObjs, ok := dbObjs.Schemas[g.Schema]; ok {
							dbObjs.Schemas[g.Schema] = schemaObjs.setGrantTo(ModeWrite, g)
						}
					}
					// ignore, we did not match the object last time we refreshed, but the grant is fine, we leave it
				} else {
					// Note that when we refreshed, toRevokeObjects was reset to an empty slice
					pd.toRevokeObjects = append(pd.toRevokeObjects, g)
				}
			}
			// Ignore; unmanaged grant
		case PrvOwnership:
			switch g.GrantedOn {
			case ObjTpTable, ObjTpView:
				if !pd.Interface.ObjectMatchers.DisjointFromObject(g.Database, g.Schema, g.Object) {
					if schemaObjs, ok := pd.Interface.aggAccountObjects.GetSchema(g.Database, g.Schema); ok {
						if aggObjAttr, ok := schemaObjs.Objects[g.Object]; ok {
							schemaObjs.Objects[g.Object] = aggObjAttr.setGrantTo(ModeWrite, g)
						}
					}
				} else if grupinDisjointFromObject(g.Database, g.Schema, g.Object) {
					// There will be no other product claiming ownership of this object, we need to
					// transfer its ownership to a role that is not managed by grupr.
					// Note that when we refreshed, toTransferOwnership was reset to an empty slice
					pd.toTransferOwnership = append(pd.toTransferOwnership, g)
					// WIP: We don't want to transfer ownership on unmanaged objects like dynamic tables!?
					// even if they are disjoint from the grupin?! cause it would be like altering an
					// unmanaged grant. How to distinguish in this case between regular tables disjoint
					// from the grupin, and, say, external, event, hybrid, etc tables? Perhaps there will
					// be no other way than to do another, ad hoc, query, to find out.
					// Even that is not simple, however, because the way to query it depends on what it is,
					// so that would be a piece of code that would have to try different kinds of strategies
					// until it finds something. e.g., try DESCRIBE TABLE, then try DESCRIBE DYNAMIC TABLE, etc.
					// Or do a SHOW OBJECTS, covering regular, hybrid, dynamic, and iceberg tables, and then
					// if you still don't find it, do external tables, event tables, interactive tables, online feature
					// tables, directory tables, etc. Actually, many of those would probably occur in the output
					// of SHOW OBJECTS; only, it would not be possible to tell them apart from regular, normal tables.
					// Unfortunately, the output does not contain a column "is_normal".
					// Perhaps in our account cache we should actually keep all kinds of tables there are,
					// so that we do have all the info, even if we are not managing grants on such types of
					// objects yet.
					// If we did that, it would be nice to have ObjType values for each kind, the only trouble
					// is that when we query for grants in Snowflake, Snowflake will always just say TABLE,
					// so when we deserialize a row like that, it would contain erroneous data. But, we'll just
					// have to take that into account, then. Actually, it's worse, for like a hybrid table
					// the granted_on column would just show TABLE, but, for example, for an interactive table,
					// there is an example in the docs where it shows as "INTERACTIVE_TABLE". Unfortunately,
					// the documentation does not provide an exhaustive list of possible values.
					//
					// And what about secure views, materialized views, and semantic views?
					//
					// You know, the Snowflake APIs for creating objects and querying grants on them have
					// some inconsistencies, probably due to the speed with which new features are added and
					// launched. That makes it harder for a tool like grupr to achieve consistent access management
					// in a simple manner.
					// Because the SHOW GRANTS APIs treat everything as a table, when I am now transfering ownership
					// away, I am already managing grants on object types I do not manage. Managing all object types
					// would make the code only simpler, actually, if a lot longer.

					// TDOD what about interactive warehouses :-) I suppose grupr should add all interactive tables
					// to all interactive warehouses that have been granted to either the product roles or product
					// roles of consumers. And of course remove the tables if usage grants on those warehouses change

					// Okay, so these Snowflake APIs are a bit of a mess actually, too bad. It's not even possible to
					// query only "normal" tables and "normal" views. We can try something here:
					// what if we say that, you know, if you have a grant here on an object that you don't match,
					// in the YAML, that's already enough to revoke it, no matter what it is. And yes, even if
					// that's ownership, you revoke it, which you do by transferring it, in this case. So, in this
					// case, yes, we chose to risk to break something: we just have to say: please don't grant anything
					// on an object to a grupr managed role unless it's matched by the relevant object expressions.
					// and if you change those expressions, or if you change the name of a product or interface, be
					// aware that those grants will be revoked.
					// Okay, and, if we have a grant here, and it is on an object matched by the YAML, but, we
					// did not find it, then maybe it is because SHOW objects did not return this type of object,
					// (the documentation is not clear on exactly what is and is not returned), and therefore,
					// by accident, by this circumstance, grupr does not support this type of object yet, and
					// therefore we will leave the grant be.
					// Okay, so now, if we try to live with not knowing the object type, then we should think about
					// what to grant. Read privileges first. SELECT will always be okay. REFERENCES is not supported for dynamic tables and
					// online feature tables. MONITOR is only relevant for certain types of tables, we might omit
					// it altogether, if you want to enable people to monitor, say, a dynamic table, you are out of luck
					// and you have to do it yourself. Now in this case, we should not revoke this privilege if it was
					// there.
					// For write privileges, we do need to revoke everything: except ownership, which is valid for all
					// types of objects.
					// You know, even if it may be very painful to write all the queries necessary to figure out exactly
					// the type of object for each and every object, it would make reasoning about correctness of the
					// code a lot easier if we made a decent attempt at exactly that.

					// ouch, you know, when it would come to implement the ability to manage grants on objects
					// exclusively, we will have to query grants on objects. And to do that, we'd definitely need
					// to know the type of each object.
					// Oh, wow, just created a hybrid table, and then queried the grants on it with these queries:
					// - show grants on table x.y.z --works;
					// - show grants on hybrid table x.y.z --error;
					// - show grants on dynamic table x.y.z --works;
					// - show grants on iceberg table x.y.z --works;
					// - show grants on interactive table x.y.z --works;
					// - show grants on external table x.y.z --works;
					// - show grants on online feature table x.y.z --works;
					//
					// Not only is it completely undocumented for this statement what the valid object types are,
					// the statement also fails to check whether the object is actually of the specified type.
					//
					// But, I guess, as Snowflake matures, eventually, eventually, they will correct their APIs,
					// and, we should aim to use the correct statements.
					// Already, as of Apr 2026, the GRANT statement is much more explicit about the possible schema level 
					// object types: https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
					//
					// We would be supporting, initially:
					//
					// DYNAMIC TABLE
					// EVENT TABLE
					// EXTERNAL TABLE
					// ICEBERG TABLE
					// INTERACTIVE TABLE
					// MATERIALIZED VIEW
					// ONLINE FEATURE TABLE
					// SEMANTIC VIEW
					// TABLE
					// VIEW
					//
					// All of these are just tables and views, really, and that's what people would expect to
					// be able to manage with a tool like grupr.
					// Even just to correctly issue GRANT and REVOKE statements for these types of objects,
					// many of which are already returned by SHOW OBJECTS (probably ;-)), we need to know
					// the exact object type
					// And we need to find out as well how these object types are represented in the output
					// of SHOW GRANTS commands, I've seen myself that HYBRID TABLE is just represented as TABLE,
					// while an INTERACTIVE TABLE is represented as INTERACTIVE_TABLE. HYBRID TABLE is also
					// not a valid object_type in the GRANT TO ROLE statement. DYNAMIC TABLE is. Can we expect
					// that the latter would appear as DYNAMIC_TABLE?
					// So to validate this, we'd have no other way than to just create at least one example
					// of each kind of table and view, hopefully the set-up costs won't be too high for any of
					// them, but it will certainly take some time to figure out how, at this moment, in all
					// the various "official" and partially documented APIs and output each of them is
					// represented, if at all.
					//
					// We could also try to limit ourselves first and say we only manage normal tables, but
					// even then we need to establish how to query only normal tables; sadly SHOW OBJECTS does
					// not have a column "is_normal", and neither does "SHOW TABLES". Querying the information
					// schema there is something called "BASE TABLE", but it might be referring to a slightly
					// different distinction, and it is an entirely different API than what we have been using
					// so far. The Snowflake REST API also mentions a table type "NORMAL", but, that API comes
					// with its own quirks as well. Anyway, I gave some feedback on the Snowflake docs,
					// and, am calling it a day.
				}
			}
			// Ignore, unmanaged grant
		}
		case PrvInsert, PrvUpdate, PrvTruncate, PrvDelete, PrvEvolveSchema, PrvApplyBudget,
        	PrvSelect, PrvSelectErrorTable, PrvReferences:
			switch g.GrantedOn {
			case ObjTpTable, ObjTpView:
				// We revoke this grant, as it is redundant in grupr it's access management model. But, we need to check
				// if we found the object in question, before we revoke: this could be a dynamic, event, external,
				// hybrid, or iceberg table, which we do not manage yet; we leave unmanaged grants intact not to break
				// anything.
				// 
				// If this is a regular table or view, and we did not find it, it means the object was created after we
				// refreshed objects, and then some of these privileges were granted.  So if we do not have the table in
				// our accountobjects in memory, we have no way of knowing for sure whether or not this is a type of
				// table we don't support yet (dynamic, iceberg, hybrid, event, external, ...) Therefore, we will not
				// revoke in that case also. If it was a regular table, the next time grupr runs, the object will be
				// found, and the privilege revoked.
				if _, ok := pd.Interface.aggAccountObjects.GetObject(g.Database, g.Schema, g.Object); ok {
					pd.toRevokeObjects = append(pd.toRevokeObjects, g)
				}
			}
			// Ignore
		// Ignore; unmanaged grant
	}
	return nil
}

func (pd *ProductDTAP) setUserManagedOwnersOfObjects(semCnf *semantics.Config, cnf *Config,
	userManagedOwners func(semantics.ProductDTAPID) map[semantics.Ident]struct{}) error {
	pd.userManagedOwnersOfObjects = map[semantics.Ident]struct{}{}
	for _, dbObjs := range pd.Interface.aggAccountObjects.DBs {
		for _, schemaObjs := range dbObjs.Schemas {
			for _, aggObjAttr := range schemaObjs.Objects {
				if slices.Contains(cnf.SystemDefinedRoles, aggObjAttr.Owner) {
					continue
				}
				if strings.HasPrefix(string(aggObjAttr.Owner), string(semCnf.Prefix)) {
					if r, err := newProductRoleFromString(semCnf, aggObjAttr.Owner); err != nil {
						return err
					} else {
						if r.Mode != ModeWrite {
							return fmt.Errorf("object was granted to grupr managed role '%v', which is not a write role, please check", r.ID)
						}
						// It's a write role
						if r.ProductID != pd.ProductID || r.DTAP != pd.DTAP {
							// So, another write role owned this object before, we need to check what user managed roles
							// have been granted this other write role; they would lose ownership of the object if we
							// would claim it; so we need to grant our write role to those user managed roles, if any
							for curOwner := range userManagedOwners(semantics.ProductDTAPID{ProductID: r.ProductID, DTAP: r.DTAP}) {
								pd.userManagedOwnersOfObjects[curOwner] = struct{}{}
							}
							continue
						}
						// We own this object, all good here, move on.
						continue
					}
				}
				// It's a user managed role, we add it
				pd.userManagedOwnersOfObjects[aggObjAttr.Owner] = struct{}{}
			}
		}
	}
	return nil
}

func (pd *ProductDTAP) grant_(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool,
	userManagedOwners func(semantics.ProductDTAPID) map[semantics.Ident]struct{}, c *accountCache) error {
	// First get the objects that are there in the account
	if err := pd.refresh(ctx, semCnf, cnf, conn, c); err != nil {
		return err
	}

	// Write grants go first, so that we do not have to copy all the read privileges we're about to set when granting ownership.
	// As with read grants, future grants go first
	if err := pd.setFutureGrantsToWriteRole(ctx, cnf, conn, productRoles); err != nil {
		return err
	}
	if err := DoFutureGrants(ctx, cnf, conn, pd.getToDoFutureGrantsToWriteRole()); err != nil {
		return err
	}

	// Now, regular grants to the write role
	if err := pd.setGrantsToWriteRole(ctx, cnf, conn, grupinDisjointFromObject, productRoles); err != nil {
		return err
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsToWriteRole()); err != nil {
		return err
	}
	// We do ownership separately; we don't do them in batches, cause they can take longer due to copying outbound grants;
	// they can even time-out for that reason, as mentioned in a 2025 version of Snowflake its documentation. We do them
	// one by one.
	//
	// Note that we grant objects directly to the product role, not via intermediate database roles; this is because we do
	// not want database roles showing up as grantor, it's just confusing; objects should have a single owning actual role.
	//
	// Before we actually grant ownership to the write role, grant the write role itself to all the current owners of the objects
	// of interest. This way, they will not lose ownership, in fact, they will not lose any privilege, and running grupr will
	// not mess up any other processes that may be running.
	// We never revoke from any role; that is the job of sysadmins: when they are done with those roles, they can drop them,
	// or if the roles need to be retained for other purposes, they can revoke this product dtap role from that other role.
	if err := pd.setUserManagedOwnersOfObjects(semCnf, cnf, userManagedOwners); err != nil {
		return err
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsOfWriteRoleToUserManagedRoles(semCnf, cnf)); err != nil {
		return err
	}
	// Then, make a second pass over the objects, and grant ownership to the write role.
	if err := DoGrantsIndividually(ctx, cnf, conn, pd.getToDoOwnershipGrants()); err != nil {
		return err
	}

	// Future grants next, so that as quickly as possible newly created objects will have correct privileges granted
	if err := pd.Interface.setFutureGrants(ctx, semCnf, cnf, conn, pd.ProductID, pd.DTAP, "", c); err != nil {
		return err
	}
	for iid, i := range pd.Interfaces {
		if err := i.setFutureGrants(ctx, semCnf, cnf, conn, pd.ProductID, pd.DTAP, iid, c); err != nil {
			return err
		}
	}
	if err := DoFutureGrants(ctx, cnf, conn, pd.getToDoFutureGrants()); err != nil {
		return err
	}

	// Now, regular grants
	if err := pd.Interface.setGrants(ctx, semCnf, cnf, conn, c); err != nil {
		return err
	}
	for _, i := range pd.Interfaces {
		if err := i.setGrants(ctx, semCnf, cnf, conn, c); err != nil {
			return err
		}
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrants()); err != nil {
		return err
	}

	return nil
}

func (pd *ProductDTAP) revoke_(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// We first revoke write privileges, to stop the wrong roles from creating objects asap
	// As with read privileges, we start with future privileges
	if err := DoFutureRevokes(ctx, cnf, conn, slices.Values(pd.toRevokeFutureObjects)); err != nil {
		return err
	}
	if err := DoRevokes(ctx, cnf, conn, slices.Values(pd.toRevokeObjects)); err != nil {
		return err
	}
	// Now we transfer ownership of objects that should no longer be owned by Grupr-managed roles
	// First, we check if we can unambiguously do this. If not, we log a message and do not
	// transfer ownership; sysadmins need to make some changes in Snowflake first.
	var newOwner semantics.Ident
	var hasNewOwner bool
	if len(pd.writeRoleGrantedToUserManagedRoles) == 0 {
		// There were no user managed original owners before grupr ran, who
		// would lose privileges if we transfered ownership, and it should be safe
		// then to transfer ownership to SYSADMIN
		newOwner = semantics.Ident("SYSADMIN")
		hasNewOwner = true
	} else if len(pd.writeRoleGrantedToUserManagedRoles) == 1 {
		// Before grupr ran, this user managed role had OWNERSHIP indirectly over
		// all objects owned by the product write role. If we now transfer ownership
		// to SYSADMIN, this role would loose OWNERSHIP, potentially breaking a pipeline.
		// Instead, we "give back" ownership to this user managed role
		for k := range pd.writeRoleGrantedToUserManagedRoles {
			newOwner = k
		}
		hasNewOwner = true
	}
	if hasNewOwner {
		if err := DoGrantsIndividually(ctx, cnf, conn, pd.getTransferOwnershipGrants(newOwner)); err != nil {
			return err
		}
		pd.toTransferOwnership = []Grant{}
	}
	if !hasNewOwner && len(pd.toTransferOwnership) > 0 {
		log.Printf("WARN: multiple historic owners of objects that no longer should be owned by product '%s', dtap '%s', keeping ownership", pd.ProductID, pd.DTAP)
	}

	// Next, revoke read privileges
	// Future grants are revoked first, in case objects are being concurrently created, at least those
	// object will stop receiving incorrect grants first.
	if err := DoFutureRevokes(ctx, cnf, conn, pd.getToDoFutureRevokes()); err != nil {
		return err
	}
	if err := DoRevokes(ctx, cnf, conn, pd.getToDoRevokes()); err != nil {
		return err
	}
	return nil
}

func (pd *ProductDTAP) getToDoFutureGrantsToWriteRole() iter.Seq[FutureGrant] {
	return func(yield func(FutureGrant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			if dbObjs.MatchAllSchemas {
				prvs := []PrivilegeComplete{}
				for _, p := range [2]PrivilegeComplete{
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
				} {
					if !dbObjs.hasFutureGrantTo(ModeWrite, ObjTpSchema, p) {
						prvs = append(prvs, p)
					}
				}
				if len(prvs) > 0 {
					if !yield(FutureGrant{
						Privileges:    prvs,
						GrantedOn:     ObjTpSchema,
						GrantedIn:     ObjTpDatabase,
						Database:      db,
						GrantedTo:     ObjTpRole,
						GrantedToName: pd.WriteRole.ID,
					}) {
						return
					}
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoGrantsToWriteRole() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			for schema, schemaObjs := range dbObjs.Schemas {
				prvs := []PrivilegeComplete{}
				for _, p := range [2]PrivilegeComplete{
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
				} {
					if !schemaObjs.hasGrantTo(ModeWrite, p) {
						prvs = append(prvs, p)
					}
				}
				if len(prvs) > 0 {
					if !yield(Grant{
						Privileges:    prvs,
						GrantedOn:     ObjTpSchema,
						Database:      db,
						Schema:        schema,
						GrantedTo:     ObjTpRole,
						GrantedToName: pd.WriteRole.ID,
					}) {
						return
					}
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoGrantsOfWriteRoleToUserManagedRoles(semCnf *semantics.Config, cnf *Config) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for r := range pd.userManagedOwnersOfObjects {
			if _, ok := pd.writeRoleGrantedToUserManagedRoles[r]; !ok {
				if !yield(Grant{
					Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
					GrantedOn:     ObjTpRole,
					GrantedRole:   pd.WriteRole.ID,
					GrantedTo:     ObjTpRole,
					GrantedToName: r,
				}) {
					return
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoOwnershipGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			for schema, schemaObjs := range dbObjs.Schemas {
				for obj, objAttr := range schemaObjs.Objects {
					if !objAttr.isOwnedByProductWriteRole {
						if !yield(Grant{
							Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvOwnership}},
							GrantedOn:     objAttr.ObjectType,
							Database:      db,
							Schema:        schema,
							Object:        obj,
							GrantedTo:     ObjTpRole,
							GrantedToName: pd.WriteRole.ID,
						}) {
							return
						}
					}
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoFutureGrants() iter.Seq[FutureGrant] {
	return func(yield func(FutureGrant) bool) {
		if !pd.Interface.pushToDoFutureGrants(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoFutureGrants(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getToDoGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		if !pd.Interface.pushToDoGrants(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoGrants(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getTransferOwnershipGrants(newOwner semantics.Ident) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for _, g := range pd.toTransferOwnership {
			g.GrantedToName = newOwner
			if !yield(g) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getToDoFutureRevokes() iter.Seq[FutureGrant] {
	return func(yield func(FutureGrant) bool) {
		if !pd.Interface.pushToDoFutureRevokes(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoFutureRevokes(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getToDoRevokes() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		if !pd.Interface.pushToDoRevokes(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoRevokes(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) pushObjectCounts(yield func(ObjCountsRow) bool) bool {
	if !pd.Interface.pushObjectCounts(yield, pd.ProductDTAPID, "") {
		return false
	}
	for iid, i := range pd.Interfaces {
		if !i.pushObjectCounts(yield, pd.ProductDTAPID, iid) {
			return false
		}
	}
	return true
}
