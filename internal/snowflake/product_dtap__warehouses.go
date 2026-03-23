package snowflake

import (
	"context"
	"database/sql"
	"iter"

	"github.com/rwberendsen/grupr/internal/semantics"
)

/*
In product_dtap__objects.go, we have ProductDTAP methods that deal with (privileges on) warehouses
*/

func (pd *ProductDTAP) addWarehouse(m Mode, id semantics.Ident) {
	switch m {
	case ModeRead:
		pd.ReadWarehouses[id] = [2]bool{}
	case ModeWrite:
		pd.WriteWarehouses[id] = [2]bool{}
	}
}

func (pd *ProductDTAP) hasWarehouse(m Mode, id semantics.Ident) bool {
	switch m {
	case ModeRead:
		if _, ok := pd.ReadWarehouses[id]; ok {
			return true
		}
	case ModeWrite:
		if _, ok := pd.WriteWarehouses[id]; ok {
			return true
		}
	}
	return false
}

func (pd *ProductDTAP) setWarehouseGrantedPrivilege(m Mode, id semantics.Ident, p Privilege) {
	switch m {
	case ModeRead:
		pd.ReadWarehouses[id] = setFlagPrivilegeWarehouse(pd.ReadWarehouses[id], p)
	case ModeWrite:
		pd.WriteWarehouses[id] = setFlagPrivilegeWarehouse(pd.WriteWarehouses[id], p)
	}
}

func (pd *ProductDTAP) setWarehouseGrants(ctx context.Context, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	for _, pr := range [2]ProductRole{pd.ReadRole, pd.WriteRole} {
		if _, ok := productRoles[pr]; !ok && cnf.DryRun {
			continue
		}
		for g, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pr.ID, map[GrantTemplate]struct{}{
			GrantTemplate{
				PrivilegeComplete: PrivilegeComplete{Privilege: PrvUsage},
				GrantedOn:         ObjTpWarehouse,
			}: {},
			GrantTemplate{
				PrivilegeComplete: PrivilegeComplete{Privilege: PrvOperate},
				GrantedOn:         ObjTpWarehouse,
			}: {},
		}, nil) {
			if err != nil {
				return err
			}
			// Should we have this grant?
			if pd.hasWarehouse(pr.Mode, g.Object) {
				// If yes, mark it as already granted
				pd.setWarehouseGrantedPrivilege(pr.Mode, g.Object, g.Privileges[0].Privilege)
			} else {
				// If not, add it to a list of grants to be revoked
				pd.toRevoke = append(pd.toRevoke, g)
			}
		}

	}
	return nil
}

func (pd *ProductDTAP) getToDoWarehouseGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for _, pr := range [2]ProductRole{pd.ReadRole, pd.WriteRole} {
			m := pd.ReadWarehouses
			if pr.Mode == ModeWrite {
				m = pd.WriteWarehouses
			}
			for w, flags := range m {
				prvs := []PrivilegeComplete{}
				if !hasFlagPrivilegeWarehouse(flags, PrvUsage) {
					prvs = append(prvs, PrivilegeComplete{Privilege: PrvUsage})
				}
				if !hasFlagPrivilegeWarehouse(flags, PrvOperate) {
					prvs = append(prvs, PrivilegeComplete{Privilege: PrvOperate})
				}
				if len(prvs) > 0 {
					if !yield(Grant{
						Privileges:    prvs,
						GrantedOn:     ObjTpWarehouse,
						Object:        w,
						GrantedTo:     ObjTpRole,
						GrantedToName: pr.ID,
					}) {
						return
					}
				}
			}
		}
	}
}
