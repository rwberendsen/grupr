package syntax

import (
	"fmt"
)

type ServiceAccount struct {
	ID             string               `yaml:"id"`
	IdentExpr      string               `yaml:"ident_expr"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	Deploys        []DeploySpec         `yaml:"deploys",omitempty"`
	DTAPRenderings map[string]Rendering `yaml:"dtap_renderings,omitempty"`
}

func (svc *ServiceAccount) validate(cnf *Config) error {
	for k, v := range svc.DTAPRenderings {
		if err := v.validate(); err != nil {
			return fmt.Errorf("service account '%s', dtap_rendering: '%s': %w", svc.ID, k, err)
		}
	}
	return nil
}
