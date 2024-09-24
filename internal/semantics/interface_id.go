package semantics

type InterfaceID struct {
	ID string		// empty string means it's a product interface
	ProductID   string
	ProducingServiceID string // empty string means it is not offered by a producing service
}
