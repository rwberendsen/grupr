package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Products map[string]Product
}

func NewGrupin(ctx context.Context, g semantics.Grupin) (Grupin, error) {
	// TODO: Grupin should also return error, just like syntax and semantics grupin:
	// all kinds of things can go wrong while building it, e.g., losing Snowflake connection,
	// losing grants, unexpected deletion of Snowflake objects / roles / privileges, etc,
	// and we should use the main package, the CLI, make up its mind on if its proper to
	// exit the program or not.
	r := Grupin{map[string]Product{}}
	c := newAccountCache()
	for k, v := range g.Products {
		p, err := go newProduct(ctx, v, c)
		// TODO: consider starting a thread for each product here, so we can hit Snowflake with multiple connections doing work concurrently
		// in this case, we should also be thinking about context here, or, even take it from the main program!? Think also about signals,
		// for example, we'd want to be able to catch OS signals like SIGTERM, so we can CTRL+C if we want to, or K8s can rotate pods.
		if err != nil { return r, err }
		r.Products[k] = newProduct(v, c)
	}
	return r, nil
}

func (g Grupin) String() string {
	// TODO: consider stripping this silly yaml serializing from semantics and snowflake package; it's really the domain of the syntax package only
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
