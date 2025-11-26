package snowflake

import (
	"context"
	"database/sql"

	"golang.org/x/sync/errgroup" 
	"github.com/rwberendsen/grupr/internal/semantics"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Products map[string]Product
	AccountCache *accountCache
}

func NewGrupin(ctx context.Context, db *sql.DB, g semantics.Grupin) (Grupin, error) {
	r := Grupin{Products: map[string]Product{}, AccountCache: newAccountCache(),}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(2) // TODO: make this configurable depending on environment variable
	for k, v := range g.Products {
		r.Products[k] = newProduct()
		eg.Go(func() error { return refreshProduct(ctx, v, r.Products[k], r.AccountCache) }
	}
	err := eg.Wait()
	return r, err
}

func (g Grupin) String() string {
	// TODO: consider stripping this silly yaml serializing from semantics and snowflake package; it's really the domain of the syntax package only
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
