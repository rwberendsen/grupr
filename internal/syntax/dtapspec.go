package syntax

type DTAPSpec struct {
	NonProd		[]string		`yaml:"non_prod,flow,omitempty`
	Prod		string `yaml:",omitempty"`
}
