package semantics

type GrupinDiff struct {
	Created map[string]Product     
	Deleted map[string]Product     
	Updated map[string]ProductDiff 
	// TODO: GlobalUserGroups, UserGroupMappings, when use cases need to compare them, expose them here somehow.
	Old Grupin
	New Grupin
}

func NewGrupinDiff(lhs Grupin, rhs Grupin) GrupinDiff {
	diff := GrupinDiff{
		map[string]Product{},
		map[string]Product{},
		map[string]ProductDiff{},
		lhs,
		rhs,
	}
	for k_lhs, v_lhs := range lhs.Products {
		v_rhs, ok := rhs.Products[k_lhs]
		if !ok {
			diff.Deleted[k_lhs] = v_lhs
		} else if !v_lhs.Equal(v_rhs) {
			diff.Updated[k_lhs] = ProductDiff{v_lhs, v_rhs}
		}
	}
	for k_rhs, v_rhs := range rhs.Products {
		if _, ok := lhs.Products[k_rhs]; !ok {
			diff.Created[k_rhs] = v_rhs
		}
	}
	return diff
}

type ProductDiff struct {
	Old Product
	New Product
}
