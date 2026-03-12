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
	if err := ValidateIDPart(cnf, svc.ID); err != nil {
		return err
	}
	if dtaps, err := svc.DTAPs.validateNormalize(cnf); err != nil {
		return fmt.Errorf("service account id: %s, DTAPs: %w", svc.ID, err)
	} else {
		svc.DTAPs = dtaps
	}
	for _, ds := range svc.Deploys {
		if err := ds.validate(cnf, svc.DTAPs); err != nil {
			return err
		}
	}
	for k, v := range svc.DTAPRenderings {
		if err := v.validate(); err != nil {
			return fmt.Errorf("service account '%s', dtap_rendering: '%s': %w", svc.ID, k, err)
		}
	}
	return nil
}
