package syntax

type Config struct {
	Infix string
	DefaultProdDTAPName string
}

func GetConfig() *Config {
	return &Config{Infix: "_x_", DefaultProdDTAPName: "p"}
}
