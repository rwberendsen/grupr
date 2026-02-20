package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type SchemaObjs struct {
	Objects         map[semantics.Ident]ObjAttr
	MatchAllObjects bool
}

func newSchemaObjs(db semantics.Ident, schema semantics.Ident, o SchemaObjs, om semantics.ObjMatcher) SchemaObjs {
	r := SchemaObjs{Objects: map[semantics.Ident]ObjAttr{}}
	r = r.setMatchAllObjects(db, schema, om)
	for k, v := range o.Objects {
		if !om.DisjointFromObject(db, schema, k) {
			r.Objects[k] = v
		}
	}
	return r
}

func newSchemaObjsFromMatched(m *matchedSchemaObjs) SchemaObjs {
	r := SchemaObjs{
		Objects: m.objects,
	}
	m.objects = nil // no need to retain; note that we create an AccountObjs from a matchedAccountObjs only once.
	return r
}

func (o SchemaObjs) setMatchAllObjects(db semantics.Ident, schema semantics.Ident, om semantics.ObjMatcher) SchemaObjs {
	if om.SupersetOfSchema(db, schema) {
		o.MatchAllObjects = true
	}
	return o
}

func (o SchemaObjs) hasObject(k semantics.Ident) bool {
	_, ok := o.Objects[k]
	return ok
}

func (o SchemaObjs) countByObjType(t ObjType) int {
	r := 0
	for _, v := range o.Objects {
		if v.ObjectType == t {
			r += 1
		}
	}
	return r
}

func (lhs SchemaObjs) add(rhs SchemaObjs) SchemaObjs {
	// NB: this method will alter referenced maps
	// Note that when we add together SchemaObjs, we do so within an interface,
	// where all ObjExpr are known to be disjoint from each other.
	// Therefore we do not have to worry about different ObjAttr for the same key
	if lhs.Objects == nil {
		return rhs
	}
	for k, v := range rhs.Objects {
		lhs.Objects[k] = v
	}
	lhs.MatchAllObjects = lhs.MatchAllObjects || rhs.MatchAllObjects
	return lhs
}
