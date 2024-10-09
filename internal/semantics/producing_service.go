package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ProducingService struct {
	ID string `yaml:"id"`
	ObjectsDB string `yaml:"objects_db,omitempty"`
	DTAPs DTAPSpec `yaml:"dtaps,omitempty"`
}

func newProducingService(sSyn syntax.ProducingService) ProducingService {
	sSem := ProducingService{
		ID: sSyn.ID,
		ObjectsDB: sSyn.ObjectsDB
	}
	sSem.DTAPs = newDTAPSpec(sSyn.DTAPs, sSyn.DTAPRendering)
	return sSem
}
