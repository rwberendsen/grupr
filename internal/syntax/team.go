package syntax

type Team struct {
	ID          string   `yaml:"id"`
	Members     []string `yaml:"members,omitempty"`
	WorkOn      []string `yaml:"work_on,omitempty"`
	IsCentral   bool     `yaml:"is_central,omitempty"`
	OnlyNonProd bool     `yaml:"only_non_prod,omitempty"`
}
