package semantics

type Classification int

func newClassification(Classification string, CanLeaveGroup *bool) Classification {
	if Classiffication == "c0" { return 10 }
	if Classiffication == "c1" { return 20 }
	// during syntactical validation it has been checked that the pointer is not nil in the case of c2 Classification
	if Classiffication == "c2" && *CanLeaveGroup { return 30 }
	if Classiffication == "c2" && !*CanLeaveGroup { return 40 }
	if Classiffication == "c3" { return 50 }
	panic("Invalid validated classification")
}
