package syntax

import (
	"fmt"
)

type DeploySpec struct {
	ProductID            string            `yaml:"product_id"`
	DTAPMapping          map[string]string `yaml:"dtap_mapping,omitempty"` // k: deployed dtap of product; v: dtap of svc account
	DoesNotDeployProd    bool              `yaml:"does_not_deploy_prod,omitempty"`
	DoesNotDeployNonProd []string          `yaml:"does_not_deploy_non_prod,omitempty"` // k: non-prod dtaps of product not deployed by this svc account
}

func (ds DeploySpec) validate(cnf *Config, dtaps DTAPSpec) error {
	if err := ValidateIDPart(cnf, ds.ProductID); err != nil {
		return err
	}
	if !ds.DoesNotDeployProd && !dtaps.HasProd() {
		return &FormattingError{fmt.Sprintf("deploy spec: can't deploy prod product dtap without prod svc account")}
	}
	for k, v := range ds.DTAPMapping {
		if !dtaps.HasDTAP(v) {
			return &FormattingError{fmt.Sprintf("dtap_mapping: '%s': unknown svc account dtap", k)}
		}
		if dtaps.IsProd(v) {
			return &FormattingError{fmt.Sprintf("dtap_mapping: '%s': prod dtap svc only allowed to deploy prod dtaps product")}
		}
		if err := ValidateIDPart(cnf, k); err != nil { // can't check if DTAP exists, belongs to product, will happen in semantics package
			return err
		}
	}
	notDeployed := map[string]struct{}{}
	for _, k := range ds.DoesNotDeployNonProd {
		if _, ok := notDeployed[k]; ok {
			return &FormattingError{fmt.Sprintf("does_not_deploy_non_prod: duplicate dtap")}
		}
		notDeployed[k] = struct{}{}
		if err := ValidateIDPart(cnf, k); err != nil { // can't check if DTAP exists, belongs to product, will happen in semantics package
			return err
		}
	}
	return nil
}
