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

// Package retry contains utility code for retrying database transactions.
package retry

// This code is taken from the Cacheroach project.

import (
	"context"
	"errors"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/jackc/pgx/v5/pgconn"
)

// Marker is a settable flag.
type Marker bool

// Mark sets the flag.
func (m *Marker) Mark() { *m = true }

// Marked returns the flag status.
func (m *Marker) Marked() bool { return bool(*m) }

// Execute is a wrapper around Retry that can be used for sql
// queries that don't have any return values.
func Execute(ctx context.Context, db types.Querier, query string, args ...any) error {
	return Retry(ctx, func(ctx context.Context) error {
		_, err := db.Exec(ctx, query, args...)
		return err
	})
}

// Retry is a convenience wrapper to automatically retry idempotent
// database operations that experience a transaction or or connection
// failure. The provided callback must be entirely idempotent, with
// no observable side-effects during its execution.
func Retry(ctx context.Context, idempotent func(context.Context) error) error {
	return Loop(ctx, func(ctx context.Context, _ *Marker) error {
		return idempotent(ctx)
	})
}

// inLoop is a key used by Loop to detect reentrant behavior.
type inLoop struct{}

// Loop is a convenience wrapper to automatically retry idempotent
// database operations that experience a transaction or a connection
// failure. The provided callback may indicate that it has started
// generating observable effects (e.g. sending result data) by calling
// its second parameter to disable the retry behavior.
//
// If Loop is called in a reentrant fashion, the retry behavior will be
// suppressed within an inner loop, allowing the retryable error to
// percolate into the outer loop.
func Loop(ctx context.Context, fn func(ctx context.Context, sideEffect *Marker) error) error {
	if outerMarker, ok := ctx.Value(inLoop{}).(*Marker); ok {
		return fn(ctx, outerMarker)
	}

	var sideEffect Marker
	ctx = context.WithValue(ctx, inLoop{}, &sideEffect)
	actionsCount.Inc()
	for {
		err := fn(ctx, &sideEffect)
		if err == nil || sideEffect.Marked() {
			return err
		}

		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "40001": // Serialization Failure
			case "40003": // Statement Completion Unknown
			case "08003": // Connection Does Not Exist
			case "08006": // Connection Failure
			default:
				abortedCount.WithLabelValues(pgErr.Code).Inc()
				return err
			}
			retryCount.WithLabelValues(pgErr.Code).Inc()
		} else {
			return err
		}
	}
}
