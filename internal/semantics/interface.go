package semantics

type Interface struct {
	// lowercased fields are added during validation
	matcher matcher

	// fields added by querying Snowflake
	matchedInclude accountObjs
	matchedExclude accountObjs
	matched        accountObjs
}

func (i *Interface) validate(pkey string, ikey string) error {
	if m, err := i.matcher.parse(i.Objects, i.ObjectsExclude); err != nil {
		return fmt.Errorf("invalid object matching expressions in product %s, interface %s: %s", pkey, ikey, err)
	} else {
		i.matcher = m
	}
	return nil
}

func (i *Interface) equals(j *Interface) bool {
	return i.matcher.equals(j.matcher)
}
