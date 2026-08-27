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

func SeqAddNilError[T any](seq iter.Seq[T1]) iter.Seq2[T1, error] {
	return func(yield func(T, error) bool) {
		for v := range seq {
			if !yield(v, nil) {
				return
			}
		}
	}
}
