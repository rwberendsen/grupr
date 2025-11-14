package util

import (
	"os"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

func GetGrupinFromPath(cnf *semantics.Config, path string) (semantics.Grupin, error) {
	var r semantics.Grupin
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	g, err := syntax.NewGrupin(f)
	f.Close()
	if err != nil {
		return r, err
	}
	r, err = semantics.NewGrupin(cnf, g)
	return r, err
}
