// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package logical

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

// ErrChaos is the error that will be injected by the WithChaos wrappers
// in this package.
var ErrChaos = errors.New("chaos")

// WithChaos returns a wrapper around a Dialect that will inject errors
// at various points throughout the execution.
func WithChaos(delegate Dialect, prob float32) Dialect {
	ret := &chaosDialect{
		delegate: delegate,
		prob:     prob,
	}
	if b, ok := delegate.(Backfiller); ok {
		return &chaosBackfiller{
			chaosDialect: ret,
			delegate:     b,
		}
	}
	return ret
}

// Wrap the optional Backfiller interface.
type chaosBackfiller struct {
	*chaosDialect
	delegate Backfiller
}

var (
	_ Backfiller         = (*chaosBackfiller)(nil)
	_ ConsistentCallback = (*chaosBackfiller)(nil)
	_ Dialect            = (*chaosBackfiller)(nil)
)

func (d *chaosBackfiller) BackfillInto(ctx context.Context, ch chan<- Message, state State) error {
	if rand.Float32() < d.prob {
		return doChaos("BackfillInto")
	}
	return d.delegate.BackfillInto(ctx, ch, state)
}

// This could include a *rand.Rand, but as soon as we start calling
// methods from multiple goroutines, there's no hope of repeatable
// behavior.
type chaosDialect struct {
	delegate Dialect
	prob     float32
}

var (
	_ ConsistentCallback = (*chaosDialect)(nil)
	_ Dialect            = (*chaosDialect)(nil)
	_ Lessor             = (*chaosDialect)(nil)
)

// Acquire will simulate busy leases, failures to acquire a lease, or
// delegate to a Lessor. If the delegate does not implement Lessor, a
// fake lease will be returned.
func (d *chaosDialect) Acquire(ctx context.Context) (types.Lease, error) {
	if rand.Float32() < d.prob {
		return nil, &types.LeaseBusyError{Expiration: time.Now().Add(time.Nanosecond)}
	}
	if rand.Float32() < d.prob {
		return nil, doChaos("Acquire")
	}
	if real, ok := d.delegate.(Lessor); ok {
		return real.Acquire(ctx)
	}
	ctx, cancel := context.WithCancel(ctx)
	return &fakeLease{ctx, cancel}, nil
}

func (d *chaosDialect) OnConsistent(cp stamp.Stamp) error {
	if real, ok := d.delegate.(ConsistentCallback); ok {
		// Only return a chaos error if the real dialect can fail out.
		if rand.Float32() < d.prob {
			return doChaos("OnConsistent")
		}
		return real.OnConsistent(cp)
	}
	return nil
}

func (d *chaosDialect) ReadInto(ctx context.Context, ch chan<- Message, state State) error {
	if rand.Float32() < d.prob {
		return doChaos("ReadInto")
	}
	return d.delegate.ReadInto(ctx, ch, state)
}

func (d *chaosDialect) Process(ctx context.Context, ch <-chan Message, events Events) error {
	if rand.Float32() < d.prob {
		return doChaos("Process")
	}
	return d.delegate.Process(ctx, ch, &chaosEvents{events, d.prob})
}

func (d *chaosDialect) ZeroStamp() stamp.Stamp {
	return d.delegate.ZeroStamp()
}

type chaosEvents struct {
	delegate Events
	prob     float32
}

var _ Events = (*chaosEvents)(nil)

func (e *chaosEvents) Backfill(
	ctx context.Context, loopName string, backfiller Backfiller, options ...Option,
) error {
	if rand.Float32() < e.prob {
		return doChaos("Backfill")
	}
	return e.delegate.Backfill(ctx, loopName, backfiller, options...)
}

func (e *chaosEvents) Flush(ctx context.Context) error {
	if rand.Float32() < e.prob {
		return doChaos("Flush")
	}
	return e.delegate.Flush(ctx)
}

func (e *chaosEvents) GetConsistentPoint() stamp.Stamp {
	return e.delegate.GetConsistentPoint()
}

func (e *chaosEvents) GetTargetDB() ident.Ident {
	return e.delegate.GetTargetDB()
}

// NotifyConsistentPoint implements State. It delegates to the loop and
// never injects an error.
func (e *chaosEvents) NotifyConsistentPoint(
	ctx context.Context, comparison AwaitComparison, point stamp.Stamp,
) <-chan stamp.Stamp {
	return e.delegate.NotifyConsistentPoint(ctx, comparison, point)
}

func (e *chaosEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	if rand.Float32() < e.prob {
		return doChaos("OnBegin")
	}
	return e.delegate.OnBegin(ctx, point)
}

func (e *chaosEvents) OnCommit(ctx context.Context) error {
	if rand.Float32() < e.prob {
		return doChaos("OnCommit")
	}
	return e.delegate.OnCommit(ctx)
}

func (e *chaosEvents) OnData(
	ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	if rand.Float32() < e.prob {
		return doChaos("OnData")
	}
	return e.delegate.OnData(ctx, source, target, muts)
}

func (e *chaosEvents) OnRollback(ctx context.Context, msg Message) error {
	if rand.Float32() < e.prob {
		return doChaos("OnRollback")
	}
	return e.delegate.OnRollback(ctx, msg)
}

func (e *chaosEvents) Stopping() <-chan struct{} {
	return e.delegate.Stopping()
}

func (e *chaosEvents) drain(ctx context.Context) error {
	return e.delegate.drain(ctx)
}

// doChaos is a convenient place to set a breakpoint.
func doChaos(msg string) error {
	return errors.WithMessage(ErrChaos, msg)
}

type fakeLease struct {
	ctx    context.Context
	cancel func()
}

var _ types.Lease = (*fakeLease)(nil)

func (f *fakeLease) Context() context.Context { return f.ctx }
func (f *fakeLease) Release()                 { f.cancel() }
