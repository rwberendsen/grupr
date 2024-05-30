package semantics

type GrupsDiff struct {
	Created map[string]Product
	Deleted map[string]Product
	Updated map[string]ProductDiff
}

func NewGrupsDiff(old Grups, new Grups) GrupsDiff {
	diff := GrupsDiff{map[string]Product{}, map[string]Product{}, map[string]ProductDiff{}}
	for k_old, v_old := range old.Products {
		v_new, ok := new.Products[k_old]
		if !ok {
			diff.Deleted[k_old] = v_old
		} else if equal := v_old.equals(v_new); !equal {
			diff.Updated[k_old] = ProductDiff{v_old, v_new}
		}
	}
	for k_new, v_new := range new.Products {
		_, ok := old.Products[k_new]
		if !ok {
			diff.Created[k_new] = v_new
		}
	}
	return diff
}

type ProductDiff struct {
	Old Product
	New Product
}
