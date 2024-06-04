package semantics

import "gopkg.in/yaml.v3"

type GrupsDiff struct {
	Created map[string]Product     `yaml:",omitempty"`
	Deleted map[string]Product     `yaml:",omitempty"`
	Updated map[string]ProductDiff `yaml:",omitempty"`
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

func (g GrupsDiff) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
