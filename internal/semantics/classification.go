package semantics

type Classification int

func newClassification(Classification string, CanLeaveGroup *bool) Classification {
	if Classification == "c0" { return 10 }
	if Classification == "c1" { return 20 }
	// during syntactical validation it has been checked that the pointer is not nil in the case of c2 Classification
	if Classification == "c2" && *CanLeaveGroup { return 30 }
	if Classification == "c2" && !*CanLeaveGroup { return 40 }
	if Classification == "c3" { return 50 }
	panic("Invalid validated classification")
}
