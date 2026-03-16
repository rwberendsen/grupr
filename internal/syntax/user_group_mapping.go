package syntax

type UserGroupMapping struct {
	ID      string `yaml:"id"`
	Mapping map[string]string
}
