package syntax

type Interface struct {
	ID                string `yaml:"id"`
	ProductID         string `yaml:"product_id"`
	InterfaceMetadata `yaml:",inline"`
}
