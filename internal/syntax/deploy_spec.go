package syntax

type DeploySpec struct {
	ProductID            string            `yaml:"product_id"`
	DTAPMapping          map[string]string `yaml:"dtap_mapping,omitempty"` // k: deployed dtap of product; v: dtap of svc account
	DoesNotDeployProd    bool              `yaml:"does_not_deploy_prod,omitempty"`
	DoesNotDeployNonProd []string          `yaml:"does_not_deploy_non_prod,omitempty"` // k: non-prod dtaps of product not deployed by this svc account
}
