package syntax

type DTAPSpec struct {
	Prod    *string  `yaml:",omitempty"`
	NonProd []string `yaml:"non_prod,flow,omitempty"`
	Manual  []string `yaml:"manual,flow,omitempty"`
}
