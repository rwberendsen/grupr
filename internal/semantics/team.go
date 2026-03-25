package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Team struct {
	ID          string
	Members     map[Ident]struct{}
	WorkOn      map[string]struct{}
	IsCentral   bool
	OnlyNonProd bool
}

func newTeam(cnf *Config, teamSyn syntax.Team, products map[string]Product) (Team, error) {
	team := Team{
		Members: map[Ident]struct{}{},
		WorkOn: map[string]struct{}{},
		IsCentral: teamSyn.IsCentral,
		OnlyNonProd: teamSyn.OnlyNonProd,
	}
	// Set ID
	if _, err := NewID(cnf, teamSyn.ID); err != nil {
		return team, fmt.Errorf("team: %w", err)
	} else {
		team.ID = teamSyn.ID
	}
	
	// Set Members
	for _, m := range teamSyn.Members {
		ident, err := NewIdentStripQuotesIfAny(m, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
		if err != nil {
			return team, err
		}
		if _, ok := team.Members[ident]; ok {
			return team, fmt.Errorf("members: duplicate identifier '%v'", ident)
		}
		team.Members[ident] = struct{}{}
	}

	// Set WorkOn
	for _, pID := range teamSyn.WorkOn {
		if _, ok := products[pID]; !ok {
			return team, fmt.Errorf("work_on: unknown product id '%s'", pID)
		}
		if _, ok := team.WorkOn[pID]; ok {
			return team, fmt.Errorf("work_on: duplicate product id '%s'", pID)
		}
		team.WorkOn[pID] = struct{}{}
	}
	return team, nil
}

func (lhs Team) Equal(rhs Team) bool {
	return lhs.ID == rhs.ID &&
		maps.Equal(lhs.Members, rhs.Members) &&
		maps.Equal(lhs.WorkOn, rhs.WorkOn) && 
		lhs.IsCentral == rhs.IsCentral &&
		lhs.OnlyNonProd == rhs.OnlyNonProd
}
