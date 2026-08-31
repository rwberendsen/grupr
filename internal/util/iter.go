package util

import (
	"iter"
)

func Seq2First[T1, T2 any](seq iter.Seq2[T1, T2]) iter.Seq[T1] {
	return func(yield func(T1) bool) {
		for v1, _ := range seq {
			if !yield(v1) {
				return
			}
		}
	}
}

func SeqAddNilError[T any](seq iter.Seq[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for v := range seq {
			if !yield(v, nil) {
				return
			}
		}
	}
}

// Adapted from: https://go.dev/blog/range-functions
// Filter returns a sequence that contains the elements
// of s for which f returns true.
func Filter[V any](f func(V) bool, s iter.Seq[V]) iter.Seq[V] {
    return func(yield func(V) bool) {
        for v := range s {
            if f(v) {
                if !yield(v) {
                    return
                }
            }
        }
    }
}
