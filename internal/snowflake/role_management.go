package snowflake

const Prefix = "_x_"

func DryRun(grupin semantics.Grupin, sfGrupin Grupin) []*BasicStats {
	/*
	for every product,
		for every dtap in product
			create read-only business role if it does not exist
			for every database in matched Snowflake objects
				go manage_database_role(product)
			for every interface in product
				for every database in matched Snowflake objects
					go manage_database_role(product, interface)
	for every prodruct,
		for every dtap,
			process_revoke_batches(product)
			for every interface
				process_revoke_batches(product, interface)
			
        list out every (database) role ever created by grupr (using prefix / tag / ownership!?), check if there are roles that are not in the current yaml, and drop them if they are not granted
	to any other role!?

        manage_database_role(product, interface=None)
		1. create read only database role if not exists
		2. get all objects matched in product, interface, from a (possibly stale) cache
		3. list out all existing read privileges of database role
		4. batch grant statements for all objects not yet granted, use ExecContext;
			 if error occurs, e.g., because objects were dropped concurrently,
				refresh the cache for this product (, interface)
				go back to step 2
		6. batch revoke statements for all objects currently granted but not in yaml, but return these batches, do not execute revokes yet (!)
		5. grant created role to business role

	process_revoke_batches(product, interface=None)
		revoke batches for this product its database role, use ExecContext;
			if error occurs, e.g., because objects were droppen concurrently,
				refresh the cache for this product (, interface)
				recompile the revoke batches 
        */

	r := []*BasicStats{}
	for prdId, prd := range grupin.Products {
		for e, ea := range prd.ObjectMatcher.Include {
			stats := &BasicStats{
				ProductId:  prdId,
				ObjExpr:    e,
				DTAP:       ea.DTAP,
				UserGroups: strings.Join(slices.Sorted(maps.Keys(ea.UserGroups)), ","),
				TableCount: sfGrupin.Products[prdId].Matched.Objects[e].TableCount(),
				ViewCount:  sfGrupin.Products[prdId].Matched.Objects[e].ViewCount(),
			}
			r = append(r, stats)
		}
		for intrfId, intrf := range prd.Interfaces {
			for e, ea := range intrf.ObjectMatcher.Include {
				stats := &BasicStats{
					ProductId:   prdId,
					InterfaceId: intrfId,
					ObjExpr:     e,
					DTAP:        ea.DTAP,
					UserGroups:  strings.Join(slices.Sorted(maps.Keys(ea.UserGroups)), ","),
					TableCount:  sfGrupin.Products[prdId].Interfaces[intrfId].Matched.Objects[e].TableCount(),
					ViewCount:   sfGrupin.Products[prdId].Interfaces[intrfId].Matched.Objects[e].ViewCount(),
				}
				r = append(r, stats)
			}
		}
	}
	return r
}
