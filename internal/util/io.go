package util

import (
	"io"
	"os"

	"gopkg.in/yaml.v3"
	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/snowflake"
	"github.com/rwberendsen/grupr/internal/syntax"
)

func GetGrupinFromPath(path string) (semantics.Grupin, error) {
	var r semantics.Grupin
	f := os.Open(path)
	g, err := syntax.NewGrupin(f)
	f.Close()
	if err != nil {
		return r, err
	}
	r, err = semantics.NewGrupin(g)
	return r, err

//	tmpl, err := template.New("grups").Funcs(template.FuncMap{"getEnv": getEnv}).Parse(string(data))
//	if err != nil {
//		return r, fmt.Errorf("parsing template: %s", err)
//	}
//	var rendered bytes.Buffer
//	if err := tmpl.Execute(&rendered, nil); err != nil {
//		return r, fmt.Errorf("rendering template: %s", err)
//	}
//	grups, err := syntax.NewGrups(rendered.Bytes())
//	if err != nil {
//		return r, fmt.Errorf("getting grups: %s", err)
//	}
//	r, err = semantics.NewGrups(grups)
//	if err != nil {
//		return r, fmt.Errorf("semantic error: %s", err)
//	}
//	return r, nil
}
