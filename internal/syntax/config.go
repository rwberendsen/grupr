package syntax

type Config struct {
	Infix string
}

func GetConfig() *Config {
	return &Config{Infix: "_x_"}
}
