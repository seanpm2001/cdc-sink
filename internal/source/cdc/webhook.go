// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"regexp"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/pkg/errors"
)

var (
	webhookRegex        = regexp.MustCompile(`^/(?P<targetDB>[^/]+)/(?P<targetSchema>[^/]+)$`)
	webhookTargetDB     = webhookRegex.SubexpIndex("targetDB")
	webhookTargetSchema = webhookRegex.SubexpIndex("targetSchema")
)

func (h *Handler) parseWebhookURL(url *url.URL, req *request) error {
	match := webhookRegex.FindStringSubmatch(url.Path)
	if match == nil {
		return errors.Errorf("can't parse url %s", url)
	}

	schema := ident.NewSchema(
		ident.New(match[webhookTargetDB]),
		ident.New(match[webhookTargetSchema]),
	)

	req.leaf = h.webhook
	req.target = schema
	return nil
}

// webhook responds to the v21.2 webhook scheme.
// https://www.cockroachlabs.com/docs/stable/create-changefeed.html#responses
func (h *Handler) webhook(ctx context.Context, req *request) error {
	var payload struct {
		Payload []struct {
			After   json.RawMessage `json:"after"`
			Key     json.RawMessage `json:"key"`
			Topic   string          `json:"topic"`
			Updated string          `json:"updated"`
		} `json:"payload"`
		Length   int    `json:"length"`
		Resolved string `json:"resolved"`
	}
	dec := json.NewDecoder(req.body)
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		// Empty input is a no-op.
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errors.Wrap(err, "could not decode payload")
	}
	target := req.target.(ident.Schema)
	if payload.Resolved != "" {
		timestamp, err := hlc.Parse(payload.Resolved)
		if err != nil {
			return err
		}
		req.timestamp = timestamp

		return h.resolved(ctx, req)
	}

	// Aggregate the mutations by target table. We know that the default
	// batch size for webhooks is reasonable.
	toProcess := make(map[ident.Table][]types.Mutation)

	for i := range payload.Payload {
		timestamp, err := hlc.Parse(payload.Payload[i].Updated)
		if err != nil {
			return err
		}

		table, qual, err := ident.ParseTable(payload.Payload[i].Topic, target)
		if err != nil {
			return err
		}
		// Ensure the destination table is in the target schema.
		if qual != ident.TableOnly {
			table = ident.NewTable(target.Database(), target.Schema(), table.Table())
		}

		mut := types.Mutation{
			Data: payload.Payload[i].After,
			Key:  payload.Payload[i].Key,
			Time: timestamp,
		}

		toProcess[table] = append(toProcess[table], mut)
	}

	// Create Store instances up front. The first time a target table is
	// used, the Stager must create the staging table. We want to ensure
	// that this happens before we create the transaction below.
	stores := make(map[ident.Table]types.Stager, len(toProcess))
	for table := range toProcess {
		s, err := h.Stores.Get(ctx, table)
		if err != nil {
			return err
		}
		stores[table] = s
	}

	return retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := h.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Stage or apply the per-target mutations.
		for target, muts := range toProcess {
			if h.Config.Immediate {
				applier, err := h.Appliers.Get(ctx, target)
				if err != nil {
					return err
				}
				if err := applier.Apply(ctx, tx, muts); err != nil {
					return err
				}
			} else {
				if err := stores[target].Store(ctx, tx, muts); err != nil {
					return err
				}
			}
		}

		return tx.Commit(ctx)
	})
}
