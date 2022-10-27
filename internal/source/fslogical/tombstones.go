// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fslogical

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	tombstone struct {
		collection string
		docID      string
	}
	tombstoneBatch struct {
		elts []tombstone
		ts   time.Time
	}
)

// Tombstones can be shared by a number of logical loops to effect the
// deletion of documents from a collection of tombstones when cdc-sink
// is offline. This type exists because Firestore does not have durable
// subscriptions; it is impossible to be notified of deletions while
// cdc-sink is offline. Instead of performing a mass anti-join to
// determine which documents to delete, we'll enable operators to write
// document tombstones into a separate collection.
//
// A document tombstone consists of three values: the original
// collection, the document id, and a timestamp. The timestamp allows
// tombstones to be implemented as just another logical loop and should
// be set to the ServerTimestamp sentinel value.
//
// The structure of a tombstone is as shown:
//
//	{
//	  "id": "AABBCCDD",
//	  "collection": "my-collection",
//	  "updated_at": "2022-08-11T13:01:59Z",
//	}
//
// where the property names are taken from the active Config.
//
// This implementation assumes that document ids within a single
// collection are not recycled.
type Tombstones struct {
	cfg       *Config
	coll      *firestore.CollectionRef
	deletesTo map[ident.Ident]ident.Table // Collection names to target tables.
	source    ident.Ident                 // The collection name; passed to Events.OnData.

	mu struct {
		sync.RWMutex
		cache *lru.Cache
	}
}

// IsDeleted returns true if the document is known to have been deleted.
func (t *Tombstones) IsDeleted(ref *firestore.DocumentRef) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mu.cache == nil {
		return false
	}
	_, found := t.mu.cache.Get(tombstone{ref.Parent.ID, ref.ID})
	return found
}

// NotifyDeleted adds a tombstone. This is called as an advisory message
// from a collection loop.
func (t *Tombstones) NotifyDeleted(ref *firestore.DocumentRef) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.cache != nil {
		t.mu.cache.Add(tombstone{ref.Parent.ID, ref.ID}, struct{}{})
	}
}

// ReadInto implements logical.Dialect. It parses tombstone documents
// from the source into tombstoneEvent messages.
func (t *Tombstones) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	cp, _ := state.GetConsistentPoint().(*consistentPoint)
	q := t.coll.
		OrderBy(t.cfg.UpdatedAtProperty.Raw(), firestore.Asc).
		StartAt(cp.AsTime().Truncate(time.Second))
	snaps := q.Snapshots(ctx)

	for {
		snap, err := snaps.Next()
		if err != nil {
			return errors.WithStack(err)
		}

		log.Tracef("collection %s: %d events", t.coll.ID, len(snap.Changes))

		batch := tombstoneBatch{ts: snap.ReadTime}

		for _, change := range snap.Changes {
			if change.Kind != firestore.DocumentAdded {
				continue
			}
			doc := change.Doc

			collNameVal, err := doc.DataAt(t.cfg.TombstoneCollectionProperty.Raw())
			if status.Code(err) == codes.NotFound {
				return errors.Errorf("document tombstone %s missing %s property; ignoring",
					doc.Ref.ID, t.cfg.TombstoneCollectionProperty)
			} else if err != nil {
				return errors.Wrapf(err, "tombstone %s", doc.Ref.ID)
			}
			collName, ok := collNameVal.(string)
			if !ok {
				return errors.Errorf("document tombstone %s property %s was not a string",
					doc.Ref.ID, t.cfg.TombstoneCollectionProperty)
			}

			docIDVal, err := doc.DataAt(t.cfg.DocumentIDProperty.Raw())
			if status.Code(err) == codes.NotFound {
				return errors.Errorf("document tombstone %s missing %s property; ignoring",
					doc.Ref.ID, t.cfg.DocumentIDProperty)
			} else if err != nil {
				return errors.Wrapf(err, "tombstone %s", doc.Ref.ID)
			}
			docID, ok := docIDVal.(string)
			if !ok {
				return errors.Errorf("document tombstone %s property %s was not a string",
					doc.Ref.ID, t.cfg.DocumentIDProperty)
			}

			batch.elts = append(batch.elts, tombstone{
				collection: collName,
				docID:      docID,
			})
		}

		select {
		case ch <- batch:
		case <-ctx.Done():
			return nil
		}
	}
}

// Process implements logical.Dialect and triggers row deletions.
func (t *Tombstones) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	for msg := range ch {
		if logical.IsRollback(msg) {
			return events.OnRollback(ctx, msg)
		}

		evt := msg.(tombstoneBatch)

		// Work quickly to update the in-memory map.
		t.mu.Lock()
		for _, elt := range evt.elts {
			t.mu.cache.Add(elt, struct{}{})
		}
		t.mu.Unlock()

		// Now, we'll set up the actual DB deletions.
		if err := events.OnBegin(ctx, &consistentPoint{Time: evt.ts}); err != nil {
			return err
		}

		for _, elt := range evt.elts {
			tbl, ok := t.deletesTo[ident.New(elt.collection)]
			if !ok {
				if t.cfg.TombstoneIgnoreUnmapped {
					log.WithFields(log.Fields{
						"id":         elt.docID,
						"collection": elt.collection,
					}).Trace("ignoring unmapped tombstone document")
					continue
				}
				return errors.Errorf("no target table configured for tombstone in collection %s", elt.collection)
			}

			mut, err := marshalDeletion(&firestore.DocumentRef{ID: elt.docID}, evt.ts)
			if err != nil {
				return err
			}

			if err := events.OnData(ctx, t.source, tbl, []types.Mutation{mut}); err != nil {
				return err
			}
		}

		if err := events.OnCommit(ctx); err != nil {
			return err
		}

		log.Tracef("processed %d Tombstones", len(evt.elts))
	}
	return nil
}

// ZeroStamp implements logical.Dialect.
func (t *Tombstones) ZeroStamp() stamp.Stamp { return &consistentPoint{} }
