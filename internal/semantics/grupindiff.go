package semantics

import "gopkg.in/yaml.v3"

type GrupinDiff struct {
	Created map[string]Product     `yaml:",omitempty"`
	Deleted map[string]Product     `yaml:",omitempty"`
	Updated map[string]ProductDiff `yaml:",omitempty"`
	CreatedAllowedUserGroups map[string]bool `yaml:"created_allowed_user_groups,omitempty"`
	DeletedAllowedUserGroups map[string]bool `yaml:"deleted_allowed_user_groups,omitempty"`
}

func NewGrupinDiff(old Grupin, new Grupin) GrupinDiff {
	diff := GrupinDiff{
		map[string]Product{},
		map[string]Product{},
		map[string]ProductDiff{},
		map[string]bool{},
		map[string]bool{},
	}
	for k_old, v_old := range old.Products {
		v_new, ok := new.Products[k_old]
		if !ok {
			diff.Deleted[k_old] = v_old
		} else if !v_old.Equal(v_new) {
			diff.Updated[k_old] = ProductDiff{v_old, v_new}
		}
	}
	for k_new, v_new := range new.Products {
		if _, ok := old.Products[k_new]; !ok {
			diff.Created[k_new] = v_new
		}
	}
	for k_old := range old.AllowedUserGroups {
		if _, ok := new.AllowedUserGroups[k_old]; !ok {
			diff.DeletedAllowedUserGroups[k_old] = true
		}
	}
	for k_new := range new.AllowedUserGroups {
		if _, ok := old.AllowedUserGroups[k_new]; !ok {
			diff.CreatedAllowedUserGroups[k_new] = true
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
		panic("GrupsDiff could not be marshalled")
	}
	return string(data)
}
