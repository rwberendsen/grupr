package syntax

import (
	"io"
	"fmt"

	"gopkg.in/yaml.v3"
)


type ElmntOr struct {
	ProducingService ProducingService `yaml:"producing_service,omitempty"`
	Product Product `yaml:",omitempty"`
	Interface Interface`yaml:"interface,omitempty"`
}

