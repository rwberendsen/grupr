package semantics

var validId *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

type Grups struct {
	Products map[string]Product
}

func NewGrups(g syntax.Grups) (Grups, err) {
	r := Grups{map[string]Product{}}
	for k, v := range g.Products {
		if !validId.MatchString(k) {
			return r, fmt.Errorf("invalid product id: %s", k)
		}
		if p, err := newProduct(v); err != nil {
			return r, fmt.Errorf("invalid product '%s': %s", k, err) 
		} else {
			r.Products[k] = p
		}
	}
	if err := r.allConsumedOk(); err != nil {
		return r, err
	}
	if err := r.allDisjoint(); err != nil {
		return r, err
	return r, nil
}

func (g Grups) allConsumedOk() error {
	for pid, p := range g.Products {
		for pi, _ := range p.consumes {
			if pi.Product == pid {
				return fmt.Errorf("consuming interface '%s' from own product '%s'", pi.Interface, pi.Product)
			}
			if pUpstream, ok := g.Products[pi.Product]; !ok {
				return fmt.Errorf("product '%s': consumed product '%s' not found", pid, pi.Product)
			} else if _, ok := pUpstream.Interfaces[pi.Interface]; !ok {
				return fmt.Errorf("product '%s': consumed interface '%s' from product '%s' not found", pid, pi.Interface, pi.Product)
			}
		}
	}
	return nil
}

func (g Grups) allDisjoint() error {
	keys := maps.Keys(g.Products)
	if len(keys) < 2 {
		return nil
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if !g.Products[keys[i]].disjoint(g.Products[keys[j]]) {
				return fmt.Errorf("overlapping products '%s' and '%s'", keys[i], keys[j])
			}
		}
	}
	return nil
}

