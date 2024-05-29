package semantics

type Product struct {
	// fields added by validation
	dtaps    map[string]bool
	matcher  matcher
	interfaces map[string]Interface
	consumes map[syntax.ProductInterface]bool

	// fields added by querying Snowflake
	matchedInclude accountObjs
	matchedExclude accountObjs
	matched        accountObjs
}

func (p Product) disjoint(o Product) bool {
	return true
	// TODO implement
}

func newProduct(p syntax.Product) (Product, error) {
	r := Product{
		dtaps: map[string]bool{},
		interfaces: map[string]Interface{},
		consumes: map[syntax.ProductInterface]bool{}
	}
	for _, i := range p.DTAPs {
		if !validId.MatchString(i) {
			return fmt.Errorf("invalid dtap")
		}
		if _, ok := p.dtaps[i]; ok {
			return fmt.Errorf("duplicate dtap")
		}
		p.dtaps[i] = true
	}
	for k, v := range p.Interfaces {
		if !validId.MatchString(k) {
			return fmt.Errorf("invalid interface id")
		}
		i, err := newInterface(v)
		if err := v.validate(pkey, k); err != nil {
			return err
		}
	}
	p.consumes = map[ProductInterface]bool{}
	for _, i := range p.Consumes {
		if _, ok := p.consumes[i]; ok {
			return fmt.Errorf("duplicate consumed interface id")
		}
		p.consumes[i] = true
	}
	if m, err := newMatcher(p.Objects, p.ObjectsExclude); err != nil {
		return fmt.Errorf("invalid object matching expressions in product %s: %s", pkey, err)
	} else {
		p.matcher = m
	}
	return nil
}

func (p *Product) equals(o *Product) bool {
	if equal := maps.Equal(p.dtaps, o.dtaps); !equal {
		return false
	}
	if equal := p.matcher.equals(o.matcher); !equal {
		return false
	}
	// interfaces
	for k_p, v_p := range p.Interfaces {
		v_o, ok := o.Interfaces[k_p]
		if !ok {
			return false
		}
		if equal := v_p.equals(v_o); !equal {
			return false
		}
	}
	for k_o, _ := range o.Interfaces {
		_, ok := p.Interfaces[k_o]
		if !ok {
			return false
		}
	}
	// consumes
	if equal := maps.Equal(p.consumes, o.consumes); !equal {
		return false
	}
	return true
}
