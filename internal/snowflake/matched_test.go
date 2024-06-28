package snowflake

import (
	"testing"

	"golang.org/x/exp/maps"

	"github.com/rwberendsen/grupr/internal/semantics"
)

func TestMatchPart(t *testing.T) {
	tests := []struct {
		exprPart    semantics.ExprPart
		identifiers map[string]bool
		want        map[string]bool
	}{
		{semantics.ExprPart{S: "prd_staging", IsQuoted: false}, map[string]bool{"PRD_STAGING": true}, map[string]bool{"PRD_STAGING": true}},
	}
	for _, test := range tests {
		if got := matchPart(test.exprPart, test.identifiers); !maps.Equal(got, test.want) {
			t.Errorf("matchPart(%v, %v) not equal to %v", test.exprPart, test.identifiers, test.want)
		}
	}
}
