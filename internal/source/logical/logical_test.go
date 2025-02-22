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

package logical_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testLogicalSmoke(t, false, false, false) })
	t.Run("consistent-backfill", func(t *testing.T) { testLogicalSmoke(t, true, false, false) })
	t.Run("consistent-chaos", func(t *testing.T) { testLogicalSmoke(t, false, false, true) })
	t.Run("consistent-chaos-backfill", func(t *testing.T) { testLogicalSmoke(t, true, false, true) })
	t.Run("immediate", func(t *testing.T) { testLogicalSmoke(t, false, true, false) })
	t.Run("immediate-chaos", func(t *testing.T) { testLogicalSmoke(t, false, true, true) })
}

func testLogicalSmoke(t *testing.T, allowBackfill, immediate, withChaos bool) {
	log.SetLevel(log.TraceLevel)
	a := assert.New(t)

	// Create a basic test fixture.
	fixture, cancel, err := base.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TestDB.Ident()
	pool := fixture.TargetPool

	// Create some tables.
	tgts := []ident.Table{
		ident.NewTable(dbName, ident.Public, ident.New("t1")),
		ident.NewTable(dbName, ident.Public, ident.New("t2")),
		ident.NewTable(dbName, ident.Public, ident.New("t3")),
		ident.NewTable(dbName, ident.Public, ident.New("t4")),
	}

	for _, tgt := range tgts {
		var schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)`, tgt)
		if _, err := pool.Exec(ctx, schema); !a.NoError(err) {
			return
		}
	}

	gen := newGenerator(tgts)
	const numEmits = 100
	gen.emit(numEmits)

	var dialect logical.Dialect = gen
	if withChaos {
		dialect = logical.WithChaos(gen, 0.05)
	}

	cfg := &logical.BaseConfig{
		ApplyTimeout:   time.Second, // Increase to make using the debugger easier.
		LoopName:       "generator",
		Immediate:      immediate,
		RetryDelay:     time.Nanosecond,
		StagingDB:      fixture.StagingDB.Ident(),
		StandbyTimeout: 5 * time.Millisecond,
		TargetConn:     pool.Config().ConnString(),
		TargetDB:       dbName,
	}
	if allowBackfill {
		cfg.BackfillWindow = time.Minute
	}

	loop, cancelLoop, err := logical.Start(ctx, cfg, dialect)
	if !a.NoError(err) {
		return
	}
	defer cancelLoop()

	// Start a goroutine to await the end consistent point.
	endConsistent := make(chan stamp.Stamp, 1)
	go func() {
		defer close(endConsistent)

		found, err := loop.AwaitConsistentPoint(ctx, logical.AwaitGTE, &fakeMessage{Index: numEmits - 1})
		if a.NoError(err) {
			endConsistent <- found
		}
	}()

	// Wait for replication.
	for _, tgt := range tgts {
		for {
			var count int
			if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tgt)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Tracef("backfill count %d", count)
			if count == numEmits {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Wait for the loop to shut down, or a timeout.
	cancelLoop()
	gen.emit(0) // Kick the simplistic ReadInto loop so that it exits.
	select {
	case <-loop.Stopped():
	case <-time.After(time.Second):
		a.Fail("timed out waiting for shutdown")
	}
	if !withChaos && !allowBackfill {
		a.Equal(int32(1), atomic.LoadInt32(&gen.atomic.processExits))
		a.Equal(int32(1), atomic.LoadInt32(&gen.atomic.readIntoExits))
	}

	select {
	case <-endConsistent:
	case <-time.After(time.Second):
		a.Fail("did not find awaited consistent point")
	}

	// Verify that we did drain the generator.
	gen.readIntoMu.Lock()
	defer gen.readIntoMu.Unlock()
	a.Equal(numEmits, gen.readIntoMu.lastBatchSent)

	// Verify that we saw all messages.
	gen.processMu.Lock()
	defer gen.processMu.Unlock()
	// Verify that we saw each unique key at least once. The actual
	// slice of messages will contain repeated entries in the chaos
	// tests.
	found := make(map[int]struct{})
	for _, msg := range gen.processMu.messages {
		if fake, ok := msg.(fakeMessage); ok {
			found[fake.Index] = struct{}{}
		}
	}
	a.Len(found, numEmits)
}

// TestUserScript injects user-provided logic into a loop.
func TestUserScript(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	// Create a basic test fixture.
	fixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TestDB.Ident()
	pool := fixture.TargetPool

	// Create some tables.
	tgts := []ident.Table{
		ident.NewTable(dbName, ident.Public, ident.New("t_1")),
		ident.NewTable(dbName, ident.Public, ident.New("t_2")),
	}

	for _, tgt := range tgts {
		var schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)`, tgt)
		_, err := pool.Exec(ctx, schema)
		r.NoError(err)
	}

	cfg := &logical.BaseConfig{
		ApplyTimeout:   2 * time.Minute, // Increase to make using the debugger easier.
		LoopName:       "generator",
		Immediate:      false,
		StagingDB:      fixture.StagingDB.Ident(),
		StandbyTimeout: 5 * time.Millisecond,
		TargetConn:     pool.Config().ConnString(),
		TargetDB:       dbName,

		ScriptConfig: script.Config{
			MainPath: "/main.ts",
			FS: &fstest.MapFS{
				"main.ts": &fstest.MapFile{Data: []byte(`
import * as api from "cdc-sink@v1";
api.configureSource("t1", {
  dispatch: (doc) => ({
    "t_1": [ doc ],
    "t_2": [ doc ]
  }),
  deletesTo: "t_1"
});
api.configureTable("t_1", {
  map: (doc) => {
    doc.v = "cowbell";
    return doc;
  }
});
api.configureTable("t_2", {
  map: (doc) => {
    doc.v = "llebwoc";
    return doc;
  }
});
`)}}}}

	// Create a generator for the upstream names.
	gen := newGenerator([]ident.Table{
		ident.NewTable(dbName, ident.Public, ident.New("t1")),
	})
	const numEmits = 100
	gen.emit(numEmits)

	_, cancelLoop, err := logical.Start(ctx, cfg, gen)
	r.NoError(err)
	defer cancelLoop()

	// Wait for replication.
	for idx, tgt := range tgts {
		var search string
		switch idx {
		case 0:
			search = "cowbell"
		case 1:
			search = "llebwoc"
		}
		for {
			var count int
			r.NoError(pool.QueryRow(ctx, fmt.Sprintf(
				"SELECT count(*) FROM %s WHERE v = $1", tgt), search).Scan(&count))
			log.Tracef("backfill count %d", count)
			if count == numEmits {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Ensure that deletes propagate correctly to t_1.
	_, err = pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE TRUE", tgts[0]))
	r.NoError(err)

	for {
		count, err := base.GetRowCount(ctx, fixture.TargetPool, tgts[0])
		r.NoError(err)
		if count == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify that t_2 was unchanged.
	count, err := base.GetRowCount(ctx, fixture.TargetPool, tgts[1])
	r.NoError(err)
	a.Equal(100, count)

}
