package semantics

type grupsDiff struct {
	created map[string]*Product
	deleted map[string]*Product
	updated map[string]productDiff
}

func (grupsDiff *grupsDiff) String() string {
	data, err := yaml.Marshal(grupsDiff)
	if err != nil {
		panic("grupsDiff could not be marshalled")
	}
	return string(data)
}

func getGrupsDiff(old *Grups, new *Grups) *grupsDiff {
	if old == nil {
		return &grupsDiff{new.Products, nil, nil}
	}
	diff := grupsDiff{map[string]*Product{}, map[string]*Product{}, map[string]productDiff{}}
	for k_old, v_old := range old.Products {
		v_new, ok := new.Products[k_old]
		if !ok {
			diff.deleted[k_old] = v_old
		} else if equal := v_old.equals(v_new); !equal {
			diff.updated[k_old] = productDiff{v_old, v_new}
		}
	}
	for k_new, v_new := range new.Products {
		_, ok := old.Products[k_new]
		if !ok {
			diff.created[k_new] = v_new
		}
	}
	return &diff
}

type productDiff struct {
	old Product
	new Product
}
