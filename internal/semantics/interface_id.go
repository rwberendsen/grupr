package semantics

type InterfaceID struct {
	ID string		// empty string means it's a product interface
	ProductID   string
	ProducingServiceID string // empty string means it is not offered by a producing service
}

func newInterfaceID(i syntax.InterfaceID) InterfaceID {
	// only difference is in InterfaceID ID is allowed to be an empty string; not so in syntax.InterfaceID
	return InterfaceID{i.ID, i.ProductID, i.ProducingServiceID}
}
