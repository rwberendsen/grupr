package semantics

import (
	"testing"
)

func newObjExprOrPanic(s string) ObjExpr {
	if o, err := newObjExpr(s); err == nil {
		return o
	}
	panic("error instantiating ObjExpr")
}

func TestObjExprSubsetOf(t *testing.T) {
	tests := []struct {
		lhs  ObjExpr
		rhs  ObjExpr
		want bool
	}{
		{
			lhs:  newObjExprOrPanic("*.*.*"),
			rhs:  newObjExprOrPanic("a.b.c"),
			want: false,
		},
		{
			lhs:  newObjExprOrPanic("a.b.c"),
			rhs:  newObjExprOrPanic("*.*.*"),
			want: true,
		},
		{
			lhs:  newObjExprOrPanic("*.*.c"),
			rhs:  newObjExprOrPanic("*.*.*"),
			want: true,
		},
		{
			lhs:  newObjExprOrPanic("a.*.*"),
			rhs:  newObjExprOrPanic("*.*.*"),
			want: true,
		},
		{
			lhs:  newObjExprOrPanic("a.*.*"),
			rhs:  newObjExprOrPanic("*.b.*"),
			want: false,
		},
		{
			lhs:  newObjExprOrPanic("a.b.*"),
			rhs:  newObjExprOrPanic("*.b.*"),
			want: true,
		},
		{
			lhs:  newObjExprOrPanic("a.b.c"),
			rhs:  newObjExprOrPanic("*.b.*"),
			want: true,
		},
		{
			lhs:  newObjExprOrPanic("a.b.c"),
			rhs:  newObjExprOrPanic("*.b.c"),
			want: true,
		},
		{
			lhs:  newObjExprOrPanic("a.a.c"),
			rhs:  newObjExprOrPanic("*.b.c"),
			want: false,
		},
	}
	for _, test := range tests {
		if test.want != test.lhs.subsetOf(test.rhs) {
			t.Errorf("\"%v.\".subsetOf(\"%v\") not equal to %v", test.lhs, test.rhs, test.want)
		}
	}
}
