package semantics

import (
	"fmt"
	"maps"
	"slices"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ServiceAccount struct {
	ID      string
	Idents	map[string]Ident             // k: dtap; v: ident
	DTAPs   DTAPSpec
	Deploys map[string]map[string]string // k: product id; v: dtap mapping
}

func newServiceAccount(cnf *Config, svcSyn syntax.ServiceAccount, products map[string]Product) (ServiceAccount, error) {
	svcSem := ServiceAccount{ID: svcSyn.ID}

	// Set DTAPs
	if dtaps, err := newDTAPSpec(cnf, svcSyn.DTAPs, svcSyn.DTAPRenderings); err != nil {
		return svcSem, err
	} else {
		svcSem.DTAPs = dtaps
	}

	// Set Idents
	renderings, err := renderTmplDataDTAP(svcSyn.Ident, util.Seq2First(svcSem.DTAPs.All()), svcSem.DTAPs.DTAPRenderings)
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
			if ident, err := NewIdentStripQuotesIfAny(cnf, s); err != nil {
				return svcSem, err
			} else {
				svcSem.Idents[ea.DTAP] = ident
			}
		}
	}

	// Set Deploys
	pSem.Deploys = map[string]map[string]string{}
	for _, ds := range svcSyn.Deploys {
		if _, ok := svcSem.Deploys[ds.ProductID]; ok {
			return svcSem, fmt.Errorf("deploy spec: duplicate product id '%s'", ds.ProductID)
		}
		
		pSem, ok := products[ds.ProductID]
		if !ok {
			return svcSem, fmt.Errorf("deploy spec: unknown product id '%s'", ds.ProductID)
		}

		for dtapProduct, dtapSVC := range ds.DTAPMappping {
			if !pSem.DTAPs.HasDTAP(dtapProduct) {
				return svcSem, fmt.Errorf("deploy spec: product id '%s': unknown dtap '%s'", pSem.ID, dtapProduct)
			}
			if pSem.DTAPs.IsProd(dtapProduct) {
				return svcSem, fmt.Errorf("deploy spec: product id '%s': non prod dtap '%s' not allowed to deploy prod product dtap '%s'", pSem.ID, dtapSVC, dtapProduct)
			}
		}

		if !ds.DoesNotDeployProd && !pSem.DTAPs.HasProd() {
			return svcSem, fmt.Errorr("deploy spec: product id '%s' has no prod dtap", pSem.ID)
		} 

		svcSem.Deploys[pSem.ID] = ds.DTAPMapping // k: dtap of product, v: dtap of service account

		if !ds.DoesNotDeployProd {
			svcSem.Deploys[pSem.ID][*pSem.DTAPs.Prod] = *svcSem.DTAPs.Prod // it was checked in syntax that svc has prod dtap
		}
	}
}

func (lhs ServiceAccount) Equal(rhs ServiceAccount) bool {
	if lhs.ID != rhs.ID {
		return false
	}
	if !lhs.DTAPs.Equal(rhs.DTAPs) {
		return false
	}
	if !maps.EqualFunc(lhs.Consumes, rhs.Consumes, maps.Equal) {
		return false
	}
	if lhs.UserGroupMappingID != rhs.UserGroupMappingID {
		return false
	}
	if !maps.EqualFunc(lhs.UserGroupRenderings, rhs.UserGroupRenderings, syntax.Rendering.Equal) {
		return false
	}
	if !lhs.InterfaceMetadata.Equal(rhs.InterfaceMetadata) {
		return false
	}
	if !lhs.UserGroupColumn.Equal(rhs.UserGroupColumn) {
		return false
	}
	if !maps.EqualFunc(lhs.Interfaces, rhs.Interfaces, InterfaceMetadata.Equal) {
		return false
	}
	return true
}
