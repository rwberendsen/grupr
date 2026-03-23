package semantics

import (
	"fmt"
	"maps"
	"slices"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
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
		Members: map[Ident]struct{},
		WorkOn: map[string]struct{},
		IsCentral: teamSyn.IsCentral,
		OnlyNonProd: teamSyn.OnlyNonProd,
	}
	// Set ID
	if _, err := NewID(cnf, teamSyn.ID); err != nil {
		return team, fmt.Errorf("team: %w", err)
	} else {
		team.ID = teamSyn.ID
	}
	
	// WIP
	// Set Idents
	renderings, err := renderTmplDataDTAP(teamSyn.IdentExpr, util.Seq2First(team.DTAPs.All()), team.DTAPs.DTAPRenderings)
	if err != nil {
		return team, err
	}
	if len(renderings) != team.DTAPs.Count() {
		return team, fmt.Errorf("number of rendered ident exprs does not match number of DTAPs")
	}
	for s, m := range renderings {
		if len(m) != 1 {
			return team, fmt.Errorf("not exactly one DTAP for rendered identifier expression")
		}
		for ea := range m {
			if ident, err := NewIdentStripQuotesIfAny(s, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr); err != nil {
				return team, err
			} else {
				team.Idents[ea.DTAP] = ident
			}
		}
	}

	// Set Deploys
	for _, ds := range teamSyn.Deploys {
		if _, ok := team.Deploys[ds.ProductID]; ok {
			return team, fmt.Errorf("deploy spec: duplicate product id '%s'", ds.ProductID)
		}

		pSem, ok := products[ds.ProductID]
		if !ok {
			return team, fmt.Errorf("deploy spec: unknown product id '%s'", ds.ProductID)
		}

		for dtapProduct, dtapSVC := range ds.DTAPMapping {
			if !pSem.DTAPs.HasDTAP(dtapProduct) {
				return team, fmt.Errorf("deploy spec: product id '%s': unknown product dtap '%s'", pSem.ID, dtapProduct)
			}
			if pSem.DTAPs.IsProd(dtapProduct) {
				return team, fmt.Errorf("deploy spec: product id '%s': no need to specify prod dtap '%s' of product in dtap mapping, can only be deployed by prd svc account anyway", pSem.ID, dtapProduct)
			}
			if !team.DTAPs.HasDTAP(dtapSVC) {
				return team, fmt.Errorf("deploy spec: product id '%s': unknown service account dtap '%s'", pSem.ID, dtapSVC)
			}
		}

		for _, dtapProduct := range ds.DoesNotDeployNonProd {
			if !pSem.DTAPs.HasDTAP(dtapProduct) || pSem.DTAPs.IsProd(dtapProduct) {
				return team, fmt.Errorf("deploy spec: does not deploy non-prod: unknown non-prod dtap '%s'", dtapProduct)
			}
		}

		team.Deploys[pSem.ID] = map[string]string{}
		for dtapProduct, isProd := range pSem.DTAPs.All() {
			if isProd {
				if !ds.DoesNotDeployProd {
					if !team.DTAPs.HasProd() {
						return team, fmt.Errorf("deploy spec: svc account does not have prod dtap, so cannot deploy prod dtap '%s' of product '%s'", dtapProduct, pSem.ID)
					}
					team.Deploys[pSem.ID][*pSem.DTAPs.Prod] = *team.DTAPs.Prod
				}
				continue
			}
			if !slices.Contains(ds.DoesNotDeployNonProd, dtapProduct) {
				if dtapSvc, ok := ds.DTAPMapping[dtapProduct]; ok {
					team.Deploys[pSem.ID][dtapProduct] = dtapSvc
				} else {
					if !team.DTAPs.HasDTAP(dtapProduct) {
						return team, fmt.Errorf("deploy spec: no same-named svc dtap to deploy non-prod dtap '%s' of product '%s', and no dtap mapping for it", dtapProduct, pSem.ID)
					}
					team.Deploys[pSem.ID][dtapProduct] = dtapProduct // default
				}
			}
		}
	}
	return team, nil
}

func (lhs ServiceAccount) Equal(rhs ServiceAccount) bool {
	if lhs.ID != rhs.ID {
		return false
	}
	if !maps.Equal(lhs.Idents, rhs.Idents) {
		return false
	}
	if !lhs.DTAPs.Equal(rhs.DTAPs) {
		return false
	}
	if !maps.EqualFunc(lhs.Deploys, rhs.Deploys, maps.Equal) {
		return false
	}
	return true
}
