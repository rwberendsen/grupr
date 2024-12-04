package semantics

import "gopkg.in/yaml.v3"

type GrupinDiff struct {
	Created map[string]Product     `yaml:",omitempty"`
	Deleted map[string]Product     `yaml:",omitempty"`
	Updated map[string]ProductDiff `yaml:",omitempty"`
	CreatedUserGroups map[string]bool `yaml:"created_allowed_user_groups,omitempty"`
	DeletedUserGroups map[string]bool `yaml:"deleted_allowed_user_groups,omitempty"`
}

func NewGrupinDiff(lhs Grupin, rhs Grupin) GrupinDiff {
	diff := GrupinDiff{
		map[string]Product{},
		map[string]Product{},
		map[string]ProductDiff{},
		map[string]bool{},
		map[string]bool{},
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
	for k_lhs := range lhs.UserGroups {
		if _, ok := rhs.UserGroups[k_lhs]; !ok {
			diff.DeletedUserGroups[k_lhs] = true
		}
	}
	// TODO: UserGroupMappings
	for k_rhs := range rhs.UserGroups {
		if _, ok := lhs.UserGroups[k_rhs]; !ok {
			diff.CreatedUserGroups[k_rhs] = true
		}
	}
	return diff
}

type ProductDiff struct {
	Old Product
	New Product
}

func (g GrupinDiff) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("GrupsDiff could not be marshalled")
	}
	return string(data)
}
