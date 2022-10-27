// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical_test

// This file contains support code for logical_test.go.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	log "github.com/sirupsen/logrus"
)

type fakeMessage struct {
	Index int       `json:"idx"`
	TS    time.Time `json:"ts"`
}

var _ stamp.Stamp = (*fakeMessage)(nil)

func (f fakeMessage) AsInt() int        { return f.Index }
func (f fakeMessage) AsTime() time.Time { return f.TS }

func (f fakeMessage) Less(other stamp.Stamp) bool {
	return f.AsInt() < other.(*fakeMessage).AsInt()
}

// generatorDialect implements logical.Dialect to generate mutations
// for a KV table.
type generatorDialect struct {
	// Send an update for each table.
	tables []ident.Table

	// Counters to ensure we shut down cleanly.
	atomic struct {
		readIntoExits int32
		processExits  int32
	}

	workRequested chan struct{}
	readIntoMu    struct {
		sync.Mutex
		totalRequested int
		lastBatchSent  int
	}

	processMu struct {
		sync.Mutex
		messages []logical.Message
	}
}

var (
	_ logical.Backfiller = (*generatorDialect)(nil)
	_ logical.Dialect    = (*generatorDialect)(nil)
)

func newGenerator(tables []ident.Table) *generatorDialect {
	return &generatorDialect{
		tables:        tables,
		workRequested: make(chan struct{}, 1),
	}
}

func (g *generatorDialect) emit(numBatches int) {
	g.readIntoMu.Lock()
	defer g.readIntoMu.Unlock()
	g.readIntoMu.totalRequested += numBatches
	select {
	case g.workRequested <- struct{}{}:
	default:
		panic("work request channel is full")
	}
}

// BackfillInto delegates to ReadInto.
func (g *generatorDialect) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return g.ReadInto(ctx, ch, state)
}

// ReadInto waits to be woken up by a call to emit, then writes
// n-many counter messages into the channel.
func (g *generatorDialect) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	log.Trace("ReadInto starting")
	defer atomic.AddInt32(&g.atomic.readIntoExits, 1)

	var nextBatchNumber int
	// If we're recovering from a failure condition, reset to a consistent point.
	if prev := state.GetConsistentPoint(); prev != nil {
		nextBatchNumber = prev.(*fakeMessage).AsInt() + 1
		log.Tracef("restarting at %d", nextBatchNumber)
	}

	for {
		g.readIntoMu.Lock()
		requested := g.readIntoMu.totalRequested
		g.readIntoMu.Unlock()

		// emit requested number of messages.
		for nextBatchNumber <= requested {
			log.Tracef("sending %d", nextBatchNumber)
			// Non-blocking send if the ctx is shut down.
			select {
			case ch <- fakeMessage{nextBatchNumber, time.Now()}:
				g.readIntoMu.Lock()
				g.readIntoMu.lastBatchSent = nextBatchNumber
				g.readIntoMu.Unlock()

			case <-ctx.Done():
				return ctx.Err()
			}

			nextBatchNumber++
		}

		// Wait for more work or to be shut down
		select {
		case <-g.workRequested:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Process triggers a transaction flow to send one mutation to each
// configured table.
func (g *generatorDialect) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	log.Trace("Process starting")
	defer atomic.AddInt32(&g.atomic.processExits, 1)

	for m := range ch {
		g.processMu.Lock()
		g.processMu.messages = append(g.processMu.messages, m)
		g.processMu.Unlock()

		// Ensure that rollbacks result in proper resynchronization.
		if logical.IsRollback(m) {
			if err := events.OnRollback(ctx, m); err != nil {
				return err
			}
			continue
		}
		msg := m.(fakeMessage)
		log.Tracef("received %d", msg.Index)

		if err := events.OnBegin(ctx, &msg); err != nil {
			return err
		}

		for _, tbl := range g.tables {
			mut := types.Mutation{
				Key:  []byte(fmt.Sprintf(`[%d]`, msg.Index)),
				Data: []byte(fmt.Sprintf(`{"k":%d,"v":"%d"}`, msg.Index, msg.Index)),
			}
			if err := events.OnData(ctx, tbl.Table(), tbl, []types.Mutation{mut}); err != nil {
				return err
			}
		}

		if err := events.OnCommit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (g *generatorDialect) ZeroStamp() stamp.Stamp {
	return &fakeMessage{}
}
