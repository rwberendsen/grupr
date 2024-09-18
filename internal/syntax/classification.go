package syntax

import (
	"fmt"
)

var clsf = map[string]bool{"co": true, "c1": true, "c2": true, "c3": true}

func validateClassification(c string, clg bool) error {
	// TODO: in the semantic layer, invalidate a product with a less strict classification than one of the interfaces that it consumes or exposes.
	if _, ok := clsf[c]; !ok { return fmt.Errorf("invalid classification: %s", c }
	if (c == "c2" && clg == nil) || (c != "c2" && clg != nil) {
		return fmt.Errorf("can leave group only applies to c2 classification")
	}
	return nil
}
