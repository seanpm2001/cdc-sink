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

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideFactory,
	ProvideLoop,
	ProvideBaseConfig,
	ProvideStagingDB,
	ProvideStagingPool,
	ProvideTargetPool,
	ProvideUserScriptConfig,
	ProvideUserScriptTarget,
)

// ProvideBaseConfig is called by wire to extract the BaseConfig from
// the dialect-specific Config.
//
// The script.Loader is referenced here so that any side effects that
// it has on the config, e.g., api.setOptions(), will be evaluated.
func ProvideBaseConfig(config Config, _ *script.Loader) (*BaseConfig, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	return config.Base(), nil
}

// ProvideFactory returns a utility which can create multiple logical
// loops.
func ProvideFactory(
	appliers types.Appliers,
	config Config,
	memo types.Memo,
	stagingPool types.StagingPool,
	targetPool types.TargetPool,
	watchers types.Watchers,
	userscript *script.UserScript,
) (*Factory, func()) {
	f := &Factory{
		appliers:    appliers,
		cfg:         config,
		memo:        memo,
		stagingPool: stagingPool,
		targetPool:  targetPool,
		watchers:    watchers,
		userscript:  userscript,
	}
	f.mu.loops = make(map[string]*Loop)
	return f, f.Close
}

// ProvideLoop is called by Wire to create a singleton replication loop.
func ProvideLoop(ctx context.Context, factory *Factory, dialect Dialect) (*Loop, error) {
	return factory.Get(ctx, dialect)
}

// ProvideStagingDB is called by Wire to retrieve the name of the
// _cdc_sink SQL DATABASE.
func ProvideStagingDB(config *BaseConfig) (ident.StagingDB, error) {
	return ident.StagingDB(config.StagingDB), nil
}

// ProvideStagingPool temporarily re-exports the target pool as the
// staging database pool.
func ProvideStagingPool(pool types.TargetPool) types.StagingPool {
	return types.StagingPool(pool)
}

// ProvideTargetPool is called by Wire to create a connection pool that
// accesses the target cluster. The pool will be closed by the cancel
// function.
func ProvideTargetPool(ctx context.Context, config *BaseConfig) (types.TargetPool, func(), error) {
	// Bring up connection to target database.
	targetCfg, err := stdpool.ParseConfig(config.TargetConn)
	if err != nil {
		return *new(types.TargetPool), nil, err
	}

	// We want to force our longest transaction time to respect the
	// apply-duration timeout and/or the backfill window. We'll take
	// the shortest non-zero value.
	txTimeout := config.ApplyTimeout
	if config.BackfillWindow > 0 &&
		(txTimeout == 0 || config.BackfillWindow < config.ApplyTimeout) {
		txTimeout = config.BackfillWindow
	}
	if txTimeout != 0 {
		rp := targetCfg.ConnConfig.RuntimeParams
		rp["idle_in_transaction_session_timeout"] = txTimeout.String()
	}

	targetPool, err := pgxpool.NewWithConfig(ctx, targetCfg)
	cancelMetrics := stdpool.PublishMetrics(targetPool)
	return types.TargetPool{Pool: targetPool}, func() {
		cancelMetrics()
		// Pool.Close is a blocking call, so it can wind up delaying
		// shutdown indefinitely. Execute it from a goroutine, so that
		// any future calls to Acquire will fail.
		go targetPool.Close()
	}, errors.Wrap(err, "could not connect to CockroachDB")
}

// ProvideUserScriptConfig is called by Wire to extract the user-script
// configuration.
func ProvideUserScriptConfig(config Config) (*script.Config, error) {
	ret := &config.Base().ScriptConfig
	return ret, ret.Preflight()
}

// ProvideUserScriptTarget is called by Wire and returns the public
// schema of the target database.
func ProvideUserScriptTarget(config *BaseConfig) script.TargetSchema {
	return script.TargetSchema(ident.NewSchema(config.TargetDB, ident.Public))
}
