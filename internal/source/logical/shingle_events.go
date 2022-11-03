// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type onDataCall struct {
	source ident.Ident
	target ident.Table
	muts   []types.Mutation
}

type shingledTx struct {
	Cancel   func()
	Closed   bool
	Context  context.Context
	Delegate *serialEvents
	OnData   chan *onDataCall
	Outcome  chan error // Emits at most one error, then is closed.
	Time     hlc.Time
}

func newShingledTx(delegate *serialEvents) *shingledTx {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &shingledTx{
		Cancel:   cancel,
		Context:  ctx,
		Delegate: delegate,
		OnData:   make(chan *onDataCall, 1024),
		Outcome:  make(chan error, 1),
		Time:     hlc.Time{},
	}
	return ret
}

// closeData will close and clear the OnData channel, to allow the
// flushLoop to exit. In the happy-path this is called from OnCommit.
// In the error case, it's called from stop.
func (e *shingledTx) closeData() {
	if !e.Closed {
		log.Warnf("XXX shingledTx %p closeData", e)
		e.Closed = true
		close(e.OnData)
	}
}

func (e *shingledTx) flushLoop() {
	defer log.Warnf("XXX shingledTx %p flushLoop exiting", e)
	defer e.Delegate.stop()
	defer close(e.Outcome)
	defer e.Cancel()
	for {
		select {
		case call, open := <-e.OnData:
			var err error
			if open {
				err = e.Delegate.OnData(e.Context, call.source, call.target, call.muts)
			} else {
				err = e.Delegate.OnCommit(e.Context)
			}
			if err != nil {
				e.Outcome <- err
				return
			}
			if !open {
				return
			}
		case <-e.Context.Done():
			return
		}
	}
}

// shingleEvents provides some degree of concurrency within the
// commit process.  It does this by shingling
type shingleEvents struct {
	*serialEvents

	toCommit chan *shingledTx // The queue of deferred data
	mu       struct {
		sync.Mutex
		currentTx *shingledTx // The current accumulator
		//		tailTime  hlc.Time    // The transaction timestamp of the previous accumulator
	}
}

var _ Events = (*shingleEvents)(nil)

func (s *shingleEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	for {
		nextTx := newShingledTx(s.serialEvents.Clone())
		if err := nextTx.Delegate.OnBegin(ctx, point); err != nil {
			nextTx.Cancel()
			return err
		}

		// Borrow the underlying connection to verify timestamp.
		// var ts string
		// if err := nextTx.Delegate.tx.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&ts); err != nil {
		// 	nextTx.Cancel()
		// 	return err
		// }
		//
		// var err error
		// nextTx.Time, err = hlc.Parse(ts)
		// if err != nil {
		// 	nextTx.Cancel()
		// 	return err
		// }

		// Make sure all commits are well-ordered.
		s.mu.Lock()
		// if hlc.Compare(nextTx.Time, s.mu.tailTime) <= 0 {
		// 	log.Tracef("choosing a new timestamp: %s <= %s", nextTx.Time, s.mu.tailTime)
		// 	s.mu.Unlock()
		//
		// 	nextTx.Cancel()
		// 	continue
		// }
		s.mu.currentTx = nextTx
		s.mu.Unlock()

		// Start the execution loop.
		go nextTx.flushLoop()

		s.toCommit <- nextTx
		return nil
	}
}

// OnCommit implements Events and adds the current tail events object
// to the queue.
func (s *shingleEvents) OnCommit(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.mu.currentTx
	if tx == nil {
		return errors.New("OnCommit called without call to OnBegin")
	}
	tx.closeData()
	s.mu.currentTx = nil

	return nil
}

// OnData implements Events and delegates to the tail events object.
func (s *shingleEvents) OnData(
	ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	s.mu.Lock()
	tail := s.mu.currentTx
	s.mu.Unlock()

	if tail == nil {
		return errors.New("OnData called without call to OnBegin")
	}
	select {
	case tail.OnData <- &onDataCall{source, target, muts}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// OnRollback implements Events and resets all nested event objects.
func (s *shingleEvents) OnRollback(ctx context.Context, msg Message) error {
	s.stop()
	return s.serialEvents.OnRollback(ctx, msg)
}

// flushLoop is executed in a goroutine. It runs until the context
// is canceled or a call to stop().
func (s *shingleEvents) flushLoop(ctx context.Context) {
	log.Warnf("XXX shingleEvents.flushLoop %p starting", s)
	defer log.Warnf("XXX shingleEvents.flushLoop %p exiting", s)

	for {
		// Get the next transaction to commit.
		var tx *shingledTx
		select {
		case tx = <-s.toCommit:
		case <-ctx.Done():
			log.Warnf("XXX shingleEvents.flushloop %p context exit", s)
			return
		}

		// Check its outcome, it may still be sending data.
		select {
		case err, _ := <-tx.Outcome:
			if err != nil {
				s.loop.setError(err)
				s.stop()
			}
		case <-ctx.Done():
			log.Warnf("XXX shingleEvents.flushloop %p context exit", s)
			return
		}
	}
}

// stop drains and cancels any pending work.
func (s *shingleEvents) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Warn("XXX shingleEvents stop")

	if tail := s.mu.currentTx; tail != nil {
		tail.Cancel()
		s.mu.currentTx = nil
	}
drain:
	for {
		select {
		case tx := <-s.toCommit:
			tx.Cancel()
		default:
			break drain
		}
	}
}
