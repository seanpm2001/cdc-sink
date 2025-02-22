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

package all

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/types"
)

// Fixture provides a complete set of database-backed services. One can
// be constructed by calling NewFixture or by incorporating TestSet into
// a Wire provider set.
type Fixture struct {
	*base.Fixture

	Appliers types.Appliers
	Configs  *apply.Configs
	Memo     types.Memo
	Stagers  types.Stagers
	Watchers types.Watchers

	Watcher types.Watcher // A watcher for TestDB.
}

// CreateTable creates a test table within the TestDB and refreshes the
// target database's Watcher. The schemaSpec parameter must have exactly
// one %s substitution parameter for the database name and table name.
func (f *Fixture) CreateTable(ctx context.Context, schemaSpec string) (base.TableInfo, error) {
	ti, err := f.Fixture.CreateTable(ctx, schemaSpec)
	if err == nil {
		err = f.Watcher.Refresh(ctx, f.TargetPool)
	}
	return ti, err
}
