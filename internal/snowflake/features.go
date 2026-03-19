package snowflake

import (
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type features struct {
	// Decoded YAML for Snowflake specific features like warehouses and stages
	warehouses []WarehouseDecoded
	// stages []StageDecoded ...
}

type ElmntOr struct {
	Warehouse *WarehouseDecoded `yaml:"warehouse,omitempty"`	
	// Stage *StageDecoded ...
}

func newFeatures(yamlPath string) (features, error) {
	feat := features{}
	f, err := os.Open(yamlPath)
	defer f.Close()
	if err != nil {
		return feat, err
	}
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	for {
		var e ElmntOr
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			return feat, fmt.Errorf("decoding Snowflake features YAML: %w", err)
		}
		nElements := 0
		if e.Warehouse != nil {
			feat.warehouses = append(feat.warehouses, e.Warehouse)
			nElements += 1
		}
		if nElements != 1 {
			return feat, fmt.Errorf("decoding Snowflake features YAML: not exactly one object", err)
		}
	}
	return feat, nil
}
