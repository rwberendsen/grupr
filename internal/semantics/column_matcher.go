package semantics

import (
	"fmt"
)

type ColumnMatcher struct {
	ColumnExpr ColumnExpr_
}

func newColumnMatcher(l []string) (ColumnMatcher, error) {
	// split with CSV reader

	// add parts on the left until we have db, schema, table, column

	// create Matcher object of db, schema, table; with only Include

	// for column; reuse ExprPart

	// for Validation: if this is not a product level interface; Matcher part of ColumnMatcher can not be
	// disjoint with interface Matcher

	// Between themselves, each column matcher must be disjoint with all other columnn matchers of the interface
	// (i.e., a column can only be user group column, masked, or hashed, but not more than one out of three.
	// Two column matchers are disjoint when their object matchers are; or if the expr parts are.

	// Perhaps rename Matcher into ObjectMatcher
}

