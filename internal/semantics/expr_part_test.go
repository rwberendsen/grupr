package semantics

import (
	"testing"
)

func TestSubsetOf(t * testing.T) {
	tests := []struct{
		lhs ExprPart
		rhs ExprPart
		want bool
	}{
		{
			lhs: ExprPart{S: "abc"},
			rhs: ExprPart{S: "abc*"},
			want: true,
		},
		{
			lhs: ExprPart{S: "abc*"},
			rhs: ExprPart{S: "abc"},
			want: false,
		},
		{
			lhs: ExprPart{S: "abc*"},
			rhs: ExprPart{S: "ab*"},
			want: true,
		},
		{
			lhs: ExprPart{S: "ab*"},
			rhs: ExprPart{S: "abc*"},
			want: false,
		},
		{
			lhs: ExprPart{S: "ab"},
			rhs: ExprPart{S: "abc*"},
			want: false,
		},
		{
			lhs: ExprPart{S: "a*"},
			rhs: ExprPart{S: "*"},
			want: true,
		},
		{
			lhs: ExprPart{S: "*"},
			rhs: ExprPart{S: "a*"},
			want: false,
		},
		{
			lhs: ExprPart{S: "a"},
			rhs: ExprPart{S: "a"},
			want: true,
		},
		{
			lhs: ExprPart{S: "a"},
			rhs: ExprPart{S: "b"},
			want: false,
		},
	}
	for _, test := range tests {
		if test.want != test.lhs.subsetOf(test.rhs) {
			t.Errorf("\"%v.\".subsetOf(\"%v\") not equal to %v", test.lhs, test.rhs, test.want)
		}
	}
}
