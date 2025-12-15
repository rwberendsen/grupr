package util

import (
	"os"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

func GetGrupinFromPath(synCnf *syntax.Config, semCnf *semantics.Config, path string) (semantics.Grupin, error) {
	var r semantics.Grupin
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	g, err := syntax.NewGrupin(synCnf, f)
	f.Close()
	if err != nil {
		return r, err
	}
	r, err = semantics.NewGrupin(semCnf, g)
	return r, err
}
