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
	Roles map[string]bool // false means no evidence from YAML (yet) that we need this role
	DatabaseRoles map[string]bool // false means no evidence from YAML (yet) that we need this database role
	// TODO: where we use map[string]bool but the bool has no meaning, use struct{} instead: more clearly meaningless
}

func NewGrupin(ctx context.Context, cnf *Config, conn *sql.DB, g semantics.Grupin) (Grupin, error) {
	r := Grupin{Products: map[string]Product{}, AccountCache: newAccountCache(),}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(cnf.NProductThreads)
	// TODO: get all (database) roles LIKE grupr_prefix, and mark them as false (no evidence yet that we need them)
	for k, v := range g.Products {
		r.Products[k] = newProduct()
		eg.Go(func() error { return refreshProduct(ctx, conn, v, r.Products[k], r.AccountCache) }
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
