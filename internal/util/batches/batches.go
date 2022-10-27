// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package batches contains support code for working with and testing
// batches of data.
package batches

import (
	"flag"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
)

const defaultSize = 100

var batchSize = flag.Int("batchSize", defaultSize, "default size for batched operations")

// Batch is a helper to perform some operation over a large number of
// values in a batch-oriented fashion. The indexes provided to the
// callback function are a half-open range [begin , end).
func Batch(count int, fn func(begin, end int) error) error {
	return Window(Size(), count, fn)
}

// Size returns the default size for batch operations. Testing code
// should generally use a multiple of this value to ensure that
// batching has been correctly implemented.
func Size() int {
	x := batchSize
	if x == nil {
		return defaultSize
	}
	return *x
}

// Window is a helper to perform some operation over a large number of
// values in a batch-oriented fashion, using the supplied batch size.
// The indexes provided to the callback function are a half-open range
// [begin , end).
func Window(batchSize, count int, fn func(begin, end int) error) error {
	idx := 0
	for {
		if batchSize > count {
			batchSize = count
		}
		if err := fn(idx, idx+batchSize); err != nil {
			return err
		}
		if batchSize == count {
			return nil
		}
		idx += batchSize
		count -= batchSize
	}
}

// The Release function must be called to return the underlying array
// back to the pool.
type Release func()

var mutationPool = &sync.Pool{New: func() any {
	x := make([]types.Mutation, 0, Size())
	return &x
}}

// Mutation returns a slice of Size() capacity.
func Mutation() ([]types.Mutation, Release) {
	ret := mutationPool.Get().(*[]types.Mutation)
	return *ret, func() { mutationPool.Put(ret) }
}
