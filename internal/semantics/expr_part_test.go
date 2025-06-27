package semantics

import (
	"testing"
)

func TestSubsetOf(t *testing.T) {
	tests := []struct {
		lhs  ExprPart
		rhs  ExprPart
		want bool
	}{
		{
			lhs:  ExprPart{S: "abc"},
			rhs:  ExprPart{S: "abc*"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "abc*"},
			rhs:  ExprPart{S: "abc"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "abc*"},
			rhs:  ExprPart{S: "ab*"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "ab*"},
			rhs:  ExprPart{S: "abc*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "ab"},
			rhs:  ExprPart{S: "abc*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "a*"},
			rhs:  ExprPart{S: "*"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "*"},
			rhs:  ExprPart{S: "a*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "a"},
			rhs:  ExprPart{S: "a"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "a"},
			rhs:  ExprPart{S: "b"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "*"},
			rhs:  ExprPart{S: "*"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "a", IsQuoted: true},
			rhs:  ExprPart{S: "a"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "a"},
			rhs:  ExprPart{S: "a", IsQuoted: true},
			want: false,
		},
		{
			lhs:  ExprPart{S: "1"},
			rhs:  ExprPart{S: "1", IsQuoted: true},
			want: false, // By design, it is up to the backend to decide what an unquoted identifier may match,
			// even if, reasonably, 1 will likely only match "1"
			// TODO: consider if we could change the definition, and say, for example, that an unquoted
			// string can be a subset of a quoted string if it cannot be expanded by the backend,
			// e.g., Snowflake, to anything else but the quoted string.
		},
		{
			lhs:  ExprPart{S: "*"},
			rhs:  ExprPart{S: "*", IsQuoted: true},
			want: false,
		},
		{
			lhs:  ExprPart{S: "A", IsQuoted: true},
			rhs:  ExprPart{S: "a"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "a", IsQuoted: true},
			rhs:  ExprPart{S: "a", IsQuoted: true},
			want: true,
		},
	}
	for _, test := range tests {
		if test.want != test.lhs.subsetOf(test.rhs) {
			t.Errorf("\"%v.\".subsetOf(\"%v\") not equal to %v", test.lhs, test.rhs, test.want)
		}
	}
}

func TestDisjoint(t *testing.T) {
	tests := []struct {
		lhs  ExprPart
		rhs  ExprPart
		want bool
	}{
		{
			lhs:  ExprPart{S: "abc"},
			rhs:  ExprPart{S: "abc*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "abc*"},
			rhs:  ExprPart{S: "abc"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "abc*"},
			rhs:  ExprPart{S: "ab*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "ab*"},
			rhs:  ExprPart{S: "abc*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "ab"},
			rhs:  ExprPart{S: "abc*"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "a*"},
			rhs:  ExprPart{S: "*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "*"},
			rhs:  ExprPart{S: "a*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "a"},
			rhs:  ExprPart{S: "a"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "a"},
			rhs:  ExprPart{S: "b"},
			want: true,
		},
		{
			lhs:  ExprPart{S: "*"},
			rhs:  ExprPart{S: "*"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "a", IsQuoted: true},
			rhs:  ExprPart{S: "a"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "a"},
			rhs:  ExprPart{S: "a", IsQuoted: true},
			want: false,
		},
		{
			lhs:  ExprPart{S: "1"},
			rhs:  ExprPart{S: "1", IsQuoted: true},
			want: false,
		},
		{
			lhs:  ExprPart{S: "*"},
			rhs:  ExprPart{S: "*", IsQuoted: true},
			want: false,
		},
		{
			lhs:  ExprPart{S: "A", IsQuoted: true},
			rhs:  ExprPart{S: "a"},
			want: false,
		},
		{
			lhs:  ExprPart{S: "a", IsQuoted: true},
			rhs:  ExprPart{S: "a", IsQuoted: true},
			want: false,
		},
	}
	for _, test := range tests {
		if test.want != test.lhs.disjoint(test.rhs) {
			t.Errorf("\"%v.\".subsetOf(\"%v\") not equal to %v", test.lhs, test.rhs, test.want)
		}
	}
}
