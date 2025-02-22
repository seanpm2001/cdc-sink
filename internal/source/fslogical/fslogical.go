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

// Package fslogical contains a logical-replication loop for streaming
// document collections from Google Cloud Firestore.
package fslogical

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Dialect reads data from Google Cloud Firestore.
type Dialect struct {
	backfillBatchSize int                      // Limit backfill query response size.
	docIDProperty     string                   // Added to mutation properties.
	fs                *firestore.Client        // Access to Firestore.
	idempotent        bool                     // Detect reprocessing the same document.
	loops             *logical.Factory         // Support dynamic nested collections.
	memo              types.Memo               // Durable logging of processed doc ids.
	pool              types.StagingPool        // Database access.
	query             firestore.Query          // The base query build from.
	recurse           bool                     // Scan for dynamic, nested collections.
	recurseFilter     map[ident.Ident]struct{} // Ignore nested collections with these names.
	sourceCollection  ident.Ident              // Identifies the loop to the user-script.
	sourcePath        string                   // The source collection path, for logging.
	tombstones        *Tombstones              // Filters already-deleted ids.
	updatedAtProperty ident.Ident              // Order-by property in queries.
}

var (
	_ logical.Backfiller = (*Dialect)(nil)
	_ logical.Dialect    = (*Dialect)(nil)
)

// These are the Dialect message types.
type (
	backfillEnd struct {
		cp *consistentPoint
	}
	batchStart struct {
		cp *consistentPoint
	}
	batchDelete struct {
		ref *firestore.DocumentRef
		ts  time.Time
	}
	batchDoc struct {
		doc *firestore.DocumentSnapshot
	}
	batchEnd struct{}
)

// BackfillInto implements logical.Dialect. It uses an ID-based cursor
// approach to scan documents in their updated-at order.
func (d *Dialect) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	prev, _ := state.GetConsistentPoint().(*consistentPoint)
	to := time.Now()
	for {
		log.Tracef("backfilling %s from %s", d.sourcePath, prev)

		err := d.backfillOneBatch(ctx, ch, to, prev, state)

		if err != nil {
			return errors.Wrap(err, d.sourcePath)
		}

		select {
		case next := <-state.NotifyConsistentPoint(ctx, logical.AwaitGT, prev):
			prev = next.(*consistentPoint)
			continue
		case <-state.Stopping():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// backfillOneBatch grabs a single batch of documents from the backend.
// It will return the next incremental consistentPoint and whether the
// backfill is expected to continue.
func (d *Dialect) backfillOneBatch(
	ctx context.Context,
	ch chan<- logical.Message,
	now time.Time,
	cp *consistentPoint,
	state logical.State,
) error {
	// We need to make the call to snaps.Next() interruptable.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-state.Stopping():
			// Cancel early to interrupt call to snaps.Next() below.
			cancel()
		case <-ctx.Done():
			// Expected path when backfillOneBatch exits.
		}
	}()

	// Iterate over the collection by (updated_at, __doc_id__) using
	// a cursor-like approach so that we can checkpoint along the way.
	q := d.query.
		OrderBy(d.updatedAtProperty.Raw(), firestore.Asc).
		OrderBy(firestore.DocumentID, firestore.Asc).
		Where(d.updatedAtProperty.Raw(), "<=", now).
		Limit(d.backfillBatchSize)
	if !cp.IsZero() {
		if cp.AsID() == "" {
			q = q.Where(d.updatedAtProperty.Raw(), ">=", cp.AsTime())
		} else {
			q = q.StartAfter(cp.AsTime(), cp.AsID())
		}
	}
	snaps := q.Snapshots(ctx)
	defer snaps.Stop()

	snap, err := snaps.Next()
	if err != nil {
		// Mask cancellation errors.
		if status.Code(err) == codes.Canceled || errors.Is(err, iterator.Done) {
			return nil
		}
		return errors.WithStack(err)
	}

	// We're going to call GetAll since we're running with a reasonable
	// limit value.  This allows us to peek at the id of the last
	// document, so we can compute the eventual consistent point for
	// this batch of docs.
	docs, err := snap.Documents.GetAll()
	if err != nil {
		return errors.WithStack(err)
	}
	log.Tracef("received %d documents from %s", len(docs), d.sourcePath)

	// Workaround / BUG? It appears that the StartAfter call above
	// sometimes returns the last document from the previous backfill
	// loop. This loop ensures that the effective consistent point
	// always goes forward in time.
	for len(docs) > 0 {
		firstCP, err := d.backfillPoint(docs[0])
		if err != nil {
			return err
		}
		if stamp.Compare(firstCP, cp) > 0 {
			break
		}
		log.Tracef("filtering")
		docs = docs[1:]
	}

	// Helper for interruptible send idiom.
	send := func(msg logical.Message) error {
		select {
		case ch <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// If we have read through the end of all documents in the
	// collection, we want the consistent-point to jump forward in time
	// to the server read-time.
	if len(docs) == 0 {
		cp = streamPoint(snap.ReadTime)
		return send(backfillEnd{cp})
	}

	// Move the proposed consistent point to the last document.
	lastDoc := docs[len(docs)-1]
	if cp, err = d.backfillPoint(lastDoc); err != nil {
		return err
	}

	// Send a batch of messages downstream.  We use a non-blocking idiom
	if err := send(batchStart{cp}); err != nil {
		return err
	}
	for _, doc := range docs {
		if err := send(batchDoc{doc}); err != nil {
			return err
		}
	}
	return send(batchEnd{})
}

// ReadInto implements logical.Dialect and subscribes to streaming
// updates from the source.
func (d *Dialect) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	// The call to snaps.Next() below needs to be made interruptable.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-state.Stopping():
			// Cancel early to interrupt call to snaps.Next() below.
			cancel()
		case <-ctx.Done():
			// Normal exit path when ReadInto exits.
		}
	}()

	cp, _ := state.GetConsistentPoint().(*consistentPoint)
	// Stream from the last updated time.
	q := d.query.
		OrderBy(d.updatedAtProperty.Raw(), firestore.Asc).
		StartAt(cp.AsTime().Truncate(time.Second))
	snaps := q.Snapshots(ctx)
	defer snaps.Stop()

	// Helper for interruptible send.
	send := func(msg logical.Message) error {
		select {
		case ch <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		log.Tracef("getting snapshot for %s", d.sourcePath)
		snap, err := snaps.Next()
		if err != nil {
			// Mask cancellations errors.
			if status.Code(err) == codes.Canceled || errors.Is(err, iterator.Done) {
				return nil
			}
			return errors.WithStack(err)
		}
		log.Tracef("collection %s: %d events", d.sourcePath, len(snap.Changes))

		if err := send(batchStart{streamPoint(snap.ReadTime)}); err != nil {
			return err
		}

		for _, change := range snap.Changes {
			switch change.Kind {
			case firestore.DocumentAdded,
				firestore.DocumentModified:
				// Ignore documents that we already know have been deleted.
				if d.tombstones.IsDeleted(change.Doc.Ref) {
					continue
				}
				if err := send(batchDoc{change.Doc}); err != nil {
					return err
				}

			case firestore.DocumentRemoved:
				d.tombstones.NotifyDeleted(change.Doc.Ref)
				if err := send(batchDelete{change.Doc.Ref, change.Doc.ReadTime}); err != nil {
					return err
				}
			}
		}

		if err := send(batchEnd{}); err != nil {
			return err
		}
	}
}

// Process implements logical.Dialect.
func (d *Dialect) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	// Only write idempotency mark when we've committed a db transaction.
	type mark struct {
		ref  *firestore.DocumentRef
		time time.Time
	}
	var toMark []mark

	for msg := range ch {
		if logical.IsRollback(msg) {
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
			continue
		}

		switch t := msg.(type) {
		case backfillEnd:
			// Just advance the consistent point.
			if err := events.OnBegin(ctx, t.cp); err != nil {
				return err
			}
			if err := events.OnCommit(ctx); err != nil {
				return err
			}

		case batchStart:
			toMark = toMark[:0]
			if err := events.OnBegin(ctx, t.cp); err != nil {
				return err
			}

		case batchDoc:
			doc := t.doc

			if ok, err := d.shouldProcess(ctx, doc.Ref, doc.UpdateTime); err != nil {
				return err
			} else if !ok {
				continue
			}

			docUpdatedAt, err := d.docUpdatedAt(doc)
			if err != nil {
				return err
			}

			mut, err := d.marshalMutation(doc, docUpdatedAt)
			if err != nil {
				return err
			}

			// Pass an empty destination table, because we know that
			// this is configured via a user-script.
			if err := events.OnData(ctx,
				d.sourceCollection, ident.Table{}, []types.Mutation{mut}); err != nil {
				return err
			}

			if d.recurse {
				if err := d.doRecurse(ctx, doc.Ref, events); err != nil {
					return err
				}
			}

			if d.idempotent {
				toMark = append(toMark, mark{doc.Ref, doc.UpdateTime})
			}

		case batchDelete:
			if ok, err := d.shouldProcess(ctx, t.ref, t.ts); err != nil {
				return err
			} else if !ok {
				continue
			}

			mut, err := marshalDeletion(t.ref, t.ts)
			if err != nil {
				return err
			}

			// Pass an empty destination table, because we know that
			// this is configured via a user-script.
			if err := events.OnData(ctx,
				d.sourceCollection, ident.Table{}, []types.Mutation{mut}); err != nil {
				return err
			}

			if d.idempotent {
				toMark = append(toMark, mark{t.ref, t.ts})
			}

		case batchEnd:
			if err := events.OnCommit(ctx); err != nil {
				return err
			}

			for _, mark := range toMark {
				if err := d.markProcessed(ctx, mark.ref, mark.time); err != nil {
					return err
				}
			}

		default:
			panic(fmt.Sprintf("unimplemented type %T", msg))
		}
	}
	return nil
}

// ZeroStamp implements logical.Dialect.
func (d *Dialect) ZeroStamp() stamp.Stamp {
	return &consistentPoint{}
}

// Compute the query-relative document start id. We need to do this so
// that sub-collections can be accessed in a consistent way.
//
// 2022-08-29: One way that does not work is to call Query.StartAfter()
// and then use Query.Serialize to hand the status over to the next
// backfill cycle.
func (d *Dialect) backfillPoint(doc *firestore.DocumentSnapshot) (*consistentPoint, error) {
	topCollection := doc.Ref.Parent
	for topCollection.Parent != nil {
		// collection -> parent doc -> parent collection
		topCollection = topCollection.Parent.Parent
	}
	relativePath := fmt.Sprintf("documents/%s/%s",
		topCollection.ID, doc.Ref.Path[len(topCollection.Path)+1:])
	updateTime, err := d.docUpdatedAt(doc)
	if err != nil {
		return nil, err
	}
	return &consistentPoint{
		BackfillID: relativePath,
		Time:       updateTime,
	}, nil
}

// docUpdatedAt extracts a timestamp from the document.
func (d *Dialect) docUpdatedAt(doc *firestore.DocumentSnapshot) (time.Time, error) {
	val, err := doc.DataAt(d.updatedAtProperty.Raw())
	if err != nil {
		return time.Time{}, errors.WithStack(err)
	}
	if t, ok := val.(time.Time); ok {
		return t, nil
	}
	return time.Time{}, errors.Errorf("document missing %q property", d.updatedAtProperty.Raw())
}

// marshalDeletion creates a mutation to represent the deletion of the
// specified document.
func marshalDeletion(id *firestore.DocumentRef, updatedAt time.Time) (types.Mutation, error) {
	key, err := json.Marshal([]string{id.ID})
	if err != nil {
		return types.Mutation{}, errors.WithStack(err)
	}

	return types.Mutation{
		Key:  key,
		Time: hlc.New(updatedAt.UnixNano(), 0),
	}, nil
}

func (d *Dialect) marshalMutation(
	doc *firestore.DocumentSnapshot, updatedAt time.Time,
) (types.Mutation, error) {
	dataMap := doc.Data()
	// Allow the doc id to be baked into the mutation.
	if d.docIDProperty != "" {
		dataMap[d.docIDProperty] = doc.Ref.ID
	}
	data, err := json.Marshal(dataMap)
	if err != nil {
		return types.Mutation{}, errors.WithStack(err)
	}

	key, err := json.Marshal([]string{doc.Ref.ID})
	if err != nil {
		return types.Mutation{}, errors.WithStack(err)
	}

	// Create empty slices so that we never pass a null value into JS.
	parentCollections := make([]string, 0)
	parentDocIds := make([]string, 0)
	for parentCollection := doc.Ref.Parent; parentCollection != nil; {
		parentCollections = append(parentCollections, parentCollection.ID)
		if parentCollection.Parent != nil {
			parentDocIds = append(parentDocIds, parentCollection.Parent.ID)
			parentCollection = parentCollection.Parent.Parent
		} else {
			break
		}
	}

	// The timestamps are converted to values that are easy to wrap
	// a JS Date around in the user script.
	// https://pkg.go.dev/github.com/dop251/goja#hdr-Handling_of_time_Time
	meta := map[string]any{
		"createTime":        doc.CreateTime.UnixNano() / 1e6,
		"id":                doc.Ref.ID,
		"parentCollections": parentCollections,
		"parentDocIds":      parentDocIds,
		"path":              doc.Ref.Path,
		"readTime":          doc.ReadTime.UnixNano() / 1e6,
		"updateTime":        doc.UpdateTime.UnixNano() / 1e6,
	}

	return types.Mutation{
		Data: data,
		Key:  key,
		Time: hlc.New(updatedAt.UnixNano(), 0),
		Meta: meta,
	}, nil
}

// doRecurse, if configured, will load dynamic sub-collections of
// the given document.
func (d *Dialect) doRecurse(
	ctx context.Context, doc *firestore.DocumentRef, events logical.Events,
) error {
	it := doc.Collections(ctx)
	for {
		coll, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return errors.Wrapf(err, "loading dynamic collections of %s", doc.Path)
		}

		if _, skip := d.recurseFilter[ident.New(coll.ID)]; skip {
			continue
		}

		fork := *d
		fork.query = coll.Query
		fork.sourcePath = coll.Path

		if err := events.Backfill(ctx, coll.Path, &fork); err != nil {
			return errors.WithMessage(err, coll.Path)
		}
	}
}

// markProcessed records an incoming document as having been processed.
func (d *Dialect) markProcessed(
	ctx context.Context, doc *firestore.DocumentRef, ts time.Time,
) error {
	payload := processedPayload{UpdatedAt: ts}
	data, err := json.Marshal(&payload)
	if err != nil {
		return errors.WithStack(err)
	}
	return d.memo.Put(ctx, d.pool, processedKey(doc), data)
}

// shouldProcess implements idempotent processing of document snapshots.
// It ensures that the update-time of any given document always
// advances.
func (d *Dialect) shouldProcess(
	ctx context.Context, doc *firestore.DocumentRef, ts time.Time,
) (bool, error) {
	if !d.idempotent {
		return true, nil
	}

	data, err := d.memo.Get(ctx, d.pool, processedKey(doc))
	if err != nil {
		return false, err
	}

	// No data means we're seeing the document for the first time.
	if data == nil {
		log.Tracef("accepting document %s at %s", doc.ID, ts)
		return true, nil
	}

	var payload processedPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return false, errors.WithStack(err)
	}

	if ts.After(payload.UpdatedAt) {
		log.Tracef("accepting document %s at %s > %s", doc.ID, ts, payload.UpdatedAt)
		return true, nil
	}

	log.Tracef("ignoring document %s at %s <= %s", doc.ID, ts, payload.UpdatedAt)
	return false, nil
}

// processedPayload is used by markProcessed and shouldProcess.
type processedPayload struct {
	UpdatedAt time.Time `json:"u,omitempty"`
}

// processedKey returns the memo key used by markProcessed and
// shouldProcess.
func processedKey(ref *firestore.DocumentRef) string {
	return fmt.Sprintf("fs-doc-%s", ref.Path)
}
