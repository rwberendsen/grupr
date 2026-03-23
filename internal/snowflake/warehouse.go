package snowflake

type WarehouseDecoded struct {
	Ident         string   `yaml:"ident"`
	Mode          string   `yaml:"mode"`
	SharedBetween []string `yaml:"shared_between,omitempty"`
	OnlyProd      bool     `yaml:"only_prod",omitempty"`
	OnlyNonProd   bool     `yaml:"only_non_prod",omitempty"`
}
