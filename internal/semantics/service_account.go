package semantics

import (
	"fmt"
	"maps"
	"slices"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
)

type ServiceAccount struct {
	ID      string
	Idents  map[string]Ident // k: dtap; v: ident
	DTAPs   DTAPSpec
	Deploys map[string]map[string]string // k: product id; v: dtap mapping; [k]k: product dtap, [k]v: service account dtap
}

func newServiceAccount(cnf *Config, svcSyn syntax.ServiceAccount, products map[string]Product) (ServiceAccount, error) {
	svcSem := ServiceAccount{
		Idents:  map[string]Ident{},
		Deploys: map[string]map[string]string{},
	}
	// Set ID
	if _, err := NewID(cnf, svcSyn.ID); err != nil {
		return svcSem, fmt.Errorf("service account: %w", err)
	} else {
		svcSem.ID = svcSyn.ID
	}

	// Set DTAPs
	if dtaps, err := newDTAPSpec(cnf, svcSyn.DTAPs, svcSyn.DTAPRenderings); err != nil {
		return svcSem, err
	} else {
		svcSem.DTAPs = dtaps
	}

	// Set Idents
	renderings, err := renderTmplDataDTAP(svcSyn.IdentExpr, util.Seq2First(svcSem.DTAPs.All()), svcSem.DTAPs.DTAPRenderings)
	if err != nil {
		return svcSem, err
	}
	if len(renderings) != svcSem.DTAPs.Count() {
		return svcSem, fmt.Errorf("number of rendered ident exprs does not match number of DTAPs")
	}
	for s, m := range renderings {
		if len(m) != 1 {
			return svcSem, fmt.Errorf("not exactly one DTAP for rendered identifier expression")
		}
		for ea := range m {
			if ident, err := NewIdentStripQuotesIfAny(s, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr); err != nil {
				return svcSem, err
			} else {
				svcSem.Idents[ea.DTAP] = ident
			}
		}
	}

	// Set Deploys
	for _, ds := range svcSyn.Deploys {
		if _, ok := svcSem.Deploys[ds.ProductID]; ok {
			return svcSem, fmt.Errorf("deploy spec: duplicate product id '%s'", ds.ProductID)
		}

		pSem, ok := products[ds.ProductID]
		if !ok {
			return svcSem, fmt.Errorf("deploy spec: unknown product id '%s'", ds.ProductID)
		}

		for dtapProduct, dtapSVC := range ds.DTAPMapping {
			if !pSem.DTAPs.HasDTAP(dtapProduct) {
				return svcSem, fmt.Errorf("deploy spec: product id '%s': unknown product dtap '%s'", pSem.ID, dtapProduct)
			}
			if pSem.DTAPs.IsProd(dtapProduct) {
				return svcSem, fmt.Errorf("deploy spec: product id '%s': no need to specify prod dtap '%s' of product in dtap mapping, can only be deployed by prd svc account anyway", pSem.ID, dtapProduct)
			}
			if !svcSem.DTAPs.HasDTAP(dtapSVC) {
				return svcSem, fmt.Errorf("deploy spec: product id '%s': unknown service account dtap '%s'", pSem.ID, dtapSVC)
			}
		}

		for _, dtapProduct := range ds.DoesNotDeployNonProd {
			if !pSem.DTAPs.HasDTAP(dtapProduct) || pSem.DTAPs.IsProd(dtapProduct) {
				return svcSem, fmt.Errorf("deploy spec: does not deploy non-prod: unknown non-prod dtap '%s'", dtapProduct)
			}
		}

		svcSem.Deploys[pSem.ID] = map[string]string{}
		for dtapProduct, isProd := range pSem.DTAPs.All() {
			if isProd {
				if !ds.DoesNotDeployProd {
					if !svcSem.DTAPs.HasProd() {
						return svcSem, fmt.Errorf("deploy spec: svc account does not have prod dtap, so cannot deploy prod dtap '%s' of product '%s'", dtapProduct, pSem.ID)
					}
					svcSem.Deploys[pSem.ID][*pSem.DTAPs.Prod] = *svcSem.DTAPs.Prod
				}
				continue
			}
			if !slices.Contains(ds.DoesNotDeployNonProd, dtapProduct) {
				if dtapSvc, ok := ds.DTAPMapping[dtapProduct]; ok {
					svcSem.Deploys[pSem.ID][dtapProduct] = dtapSvc
				} else {
					if !svcSem.DTAPs.HasDTAP(dtapProduct) {
						return svcSem, fmt.Errorf("deploy spec: no same-named svc dtap to deploy non-prod dtap '%s' of product '%s', and no dtap mapping for it", dtapProduct, pSem.ID)
					}
					svcSem.Deploys[pSem.ID][dtapProduct] = dtapProduct // default
				}
			}
		}
	}
	return svcSem, nil
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
