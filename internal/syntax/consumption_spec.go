package syntax

type ConsumptionSpec struct {
	InterfaceID       `yaml:",inline"`
	DTAPMapping       map[string]string `yaml:"dtap_mapping,omitempty"`
	NonConsumingDTAPs []string          `yaml:"non_consuming_dtaps,omitempty"`
}
