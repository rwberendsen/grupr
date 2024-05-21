package semantics

type matcher struct {
	include map[expr]bool
	exclude map[expr]bool
}

func (m matcher) parse(include []string, exclude []string) (matcher, error) {
	m.include = map[expr]bool{}
	for _, objExpr := range include {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		if _, ok := m.include[parsed]; ok {
			return m, fmt.Errorf("duplicate include expr")
		}
		m.include[parsed] = true
	}
	if ok := m.include.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of include exprs")
	}
	m.exclude = map[expr]bool{}
	for _, objExpr := range exclude {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return m, fmt.Errorf("parsing obj expr: %s", err)
		}
		if _, ok := m.exclude[parsed]; ok {
			return m, fmt.Errorf("duplicate exclude expr")
		}
		m.exclude[parsed] = true
	}
	if ok := m.exclude.allDisjoint(); !ok {
		return m, fmt.Errorf("non disjoint set of exclude exprs")
	}
	// check that every expr in exclude is a strict subset of an expression in include
	for i, _ := range m.exclude {
		hasStrictSuperset := false
		for j, _ := range m.include {
			if i.subsetOf(j) && !j.isSubsetOf(i) {
				hasStrictSuperset = true
			}
		}
		if !hasStrictSuperset {
			return m, fmt.Errorf("exclude expr without strict superset include expr")
		}
	}
}

func (lhs matcher) equals(rhs matcher) bool {
	return maps.Equals(lhs.include, rhs.include) && maps.Equals(lhs.exclude, rhs.exclude)
}
