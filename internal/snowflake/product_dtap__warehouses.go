package snowflake

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
		for g, err := range QueryFutureGrantsToRoleFiltered(ctx, conn, pr.ID, map[GrantTemplate]struct{}{
			GrantTemplate{
				PrivilegeComplete: PrivilegeComplete{Privilege: PrvUsage},
				GrantedOn: ObjTpWarehouse,
			}: {},
			GrantTemplate{
				PrivilegeComplete: PrivilegeComplete{Privilege: PrvOperate},
				GrantedOn: ObjTpWarehouse,
			}: {},
		}, nil) {
			if err != nil {
				return err
			}
			// Should we have this grant?
			if pd.hasWarehouse(pr.Mode, g.Object) {
				// If yes, mark it as already granted
				pd.setWarehouseGrantedPrivilege(pr.Mode, g.Object, g.PrivilegeComplete[0].Privilege)
			} else {
			 	// If not, add it to a list of grants to be revoked
				pd.ToRevoke = append(pd.ToRevoke, g)
			}
		}

	}
	return nil
}

func (pd *ProductDTAP) getToDoWarehouseGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for pr := range [2]ProductRole{pd.ReadRole, pd.WriteRole} {
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
						Privileges: prvs,
						GrantedOn: ObjTpWarehouse,
						Object: w,
						GrantedTo: ObjTpRole,
						GrantedToName: pr.ID,
					}) {
						return
					}
				}
			}
		}
	}
}
