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

// Package all contains a test rig for all services.
package all

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/staging"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/google/wire"
)

// TestSet contains providers to create a self-contained Fixture.
var TestSet = wire.NewSet(
	base.TestSet,
	staging.Set,
	target.Set,

	ProvideWatcher,

	wire.Struct(new(Fixture), "*"),
)

// ProvideWatcher is called by Wire to construct a Watcher
// bound to the testing database.
func ProvideWatcher(
	ctx context.Context, testDB base.TestDB, watchers types.Watchers,
) (types.Watcher, error) {
	return watchers.Get(ctx, testDB.Ident())
}
