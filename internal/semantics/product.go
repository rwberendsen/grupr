package semantics

import (
	"fmt"
	"regexp"

	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/exp/maps"
)

var validTemplate *regexp.Regexp = regexp.MustCompile(`^[A-Za-z0-9_]+$`) // empty DTAP string or UserGroup string not supported

type Product struct {
	DTAPs      map[string]bool `yaml:"dtaps,flow,omitempty"`
	UserGroups map[string]bool `yaml:"user_groups,flow,omitempty"`
	Matcher    Matcher
	Interfaces map[string]Interface             `yaml:",omitempty"`
	Consumes   map[syntax.ProductInterface]bool `yaml:",omitempty"`
}

func (lhs Product) disjoint(rhs Product) bool {
	if !lhs.Matcher.disjoint(rhs.Matcher) {
		return false
	}
	for _, l := range lhs.Interfaces {
		if !l.Matcher.disjoint(rhs.Matcher) {
			return false
		}
		for _, r := range rhs.Interfaces {
			if !r.Matcher.disjoint(lhs.Matcher) {
				return false
			}
			if !r.Matcher.disjoint(l.Matcher) {
				return false
			}
		}
	}
	return true
}

func newProduct(p syntax.Product) (Product, error) {
	r := Product{
		DTAPs:      map[string]bool{},
		UserGroups: map[string]bool{},
		Interfaces: map[string]Interface{},
		Consumes:   map[syntax.ProductInterface]bool{},
	}
	for _, i := range p.DTAPs {
		if !validTemplate.MatchString(i) {
			return r, fmt.Errorf("invalid dtap")
		}
		if _, ok := r.DTAPs[i]; ok {
			return r, fmt.Errorf("duplicate dtap")
		}
		r.DTAPs[i] = true
	}
	for _, i := range p.UserGroups {
		if !validTemplate.MatchString(i) {
			return r, fmt.Errorf("invalid user group")
		}
		if _, ok := r.UserGroups[i]; ok {
			return r, fmt.Errorf("duplicate user group")
		}
		r.UserGroups[i] = true
	}
	if m, err := newMatcher(p.Objects, p.ObjectsExclude, r.DTAPs, r.UserGroups); err != nil {
		return r, fmt.Errorf("invalid object matching expressions: %s", err)
	} else {
		r.Matcher = m
	}
	for k, v := range p.Interfaces {
		if !validId.MatchString(k) {
			return r, fmt.Errorf("invalid interface id: '%s'", k)
		}
		if i, err := newInterface(v, r.DTAPs, r.UserGroups); err != nil {
			return r, fmt.Errorf("invalid interface '%s': %s", k, err)
		} else {
			r.Interfaces[k] = i
		}
	}
	for _, i := range p.Consumes {
		if _, ok := r.Consumes[i]; ok {
			return r, fmt.Errorf("duplicate consumed interface id")
		}
		r.Consumes[i] = true
	}
	return r, nil
}

func (p Product) equals(o Product) bool {
	if equal := maps.Equal(p.DTAPs, o.DTAPs); !equal {
		return false
	}
	if equal := p.Matcher.equals(o.Matcher); !equal {
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
	for k_o := range o.Interfaces {
		_, ok := p.Interfaces[k_o]
		if !ok {
			return false
		}
	}
	// consumes
	if equal := maps.Equal(p.Consumes, o.Consumes); !equal {
		return false
	}
	return true
}
