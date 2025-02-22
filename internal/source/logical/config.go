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
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	defaultApplyTimeout   = 30 * time.Second
	defaultFanShards      = 16
	defaultRetryDelay     = 10 * time.Second
	defaultStandbyTimeout = 5 * time.Second
	defaultTargetDBConns  = 1024
	defaultBytesInFlight  = 10 * 1024 * 1024
)

// Config is implemented by dialects. This interface exists to allow coordination of
// error-checking in the Preflight method.
type Config interface {
	// Base returns the configuration.
	Base() *BaseConfig
	// Preflight validates the Config. Dialect implementations should
	// delegate to the BaseConfig's Preflight method.
	Preflight() error
}

// BaseConfig defines the core configuration required by logical.Loop.
type BaseConfig struct {
	// The maximum length of time to wait for an incoming transaction
	// to settle (i.e. to detect stalls in the target database).
	ApplyTimeout time.Duration
	// BackfillWindow enables the use of fan mode for backfilling data
	// sources if the consistent point is older than the specified
	// duration. A zero value disables the use of backfill mode.
	BackfillWindow time.Duration
	// The maximum number of raw tuple-data that has yet to be applied
	// to the target database. This will act as an approximate upper
	// bound on the amount of in-memory tuple data by pausing the
	// replication receiver until sufficient number of other mutations
	// have been applied.
	BytesInFlight int
	// Used in testing to inject errors during processing.
	ChaosProb float32
	// The default Consistent Point to use for replication.
	// Consistent Point persisted in the target database will be used, if available.
	DefaultConsistentPoint string
	// The number of concurrent connections to use when writing data in
	// fan mode.
	FanShards int
	// Support ordering updates to satisfy foreign key constraints.
	ForeignKeysEnabled bool
	// Place the configuration into immediate mode, where mutations are
	// applied without waiting for transaction boundaries.
	Immediate bool
	// Uniquely identifies the replication loop.
	LoopName string
	// The amount of time to sleep between replication-loop retries.
	// If zero, a default value will be used.
	RetryDelay time.Duration
	// Userscript configuration.
	ScriptConfig script.Config
	// How often to commit the latest consistent point.
	StandbyTimeout time.Duration
	// The name of a SQL database in the target cluster to store
	// metadata in.
	StagingDB ident.Ident
	// Connection string for the target cluster.u
	TargetConn string
	// The SQL database in the target cluster to write into.
	TargetDB ident.Ident
	// The number of connections to the target database. If zero, a
	// default value will be used.
	TargetDBConns int
}

// Base returns the BaseConfig.
func (c *BaseConfig) Base() *BaseConfig {
	return c
}

// Bind adds flags to the set.
func (c *BaseConfig) Bind(f *pflag.FlagSet) {
	c.ScriptConfig.Bind(f)

	f.DurationVar(&c.ApplyTimeout, "applyTimeout", defaultApplyTimeout,
		"the maximum amount of time to wait for an update to be applied")
	f.DurationVar(&c.BackfillWindow, "backfillWindow", 0,
		"use a high-throughput, but non-transactional mode if replication is this far behind")
	f.IntVar(&c.BytesInFlight, "bytesInFlight", defaultBytesInFlight,
		"apply backpressure when amount of in-flight mutation data reaches this limit")
	// LoopName bound by dialect packages.
	// DefaultConsistentPoint bound by dialect packages.
	f.BoolVar(&c.Immediate, "immediate", false,
		"apply data without waiting for transaction boundaries")
	f.IntVar(&c.FanShards, "fanShards", defaultFanShards,
		"the number of concurrent connections to use when writing data in fan mode")
	f.BoolVar(&c.ForeignKeysEnabled, "foreignKeys", false,
		"re-order updates to satisfy foreign key constraints")
	f.DurationVar(&c.RetryDelay, "retryDelay", defaultRetryDelay,
		"the amount of time to sleep between replication retries")
	f.Var(ident.NewValue("_cdc_sink", &c.StagingDB), "stagingDB",
		"a SQL database to store metadata in")
	f.DurationVar(&c.StandbyTimeout, "standbyTimeout", defaultStandbyTimeout,
		"how often to commit the consistent point")
	f.StringVar(&c.TargetConn, "targetConn", "", "the target cluster's connection string")
	f.Var(ident.NewValue("", &c.TargetDB), "targetDB",
		"the SQL database in the target cluster to update")
	f.IntVar(&c.TargetDBConns, "targetDBConns", defaultTargetDBConns,
		"the maximum pool size to the target cluster")
}

// Copy returns a deep copy of the Config.
func (c *BaseConfig) Copy() *BaseConfig {
	ret := *c
	return &ret
}

// Preflight ensures that unset configuration options have sane defaults
// and returns an error if the Config is missing any fields for which a
// default connot be provided.
func (c *BaseConfig) Preflight() error {
	if err := c.ScriptConfig.Preflight(); err != nil {
		return err
	}

	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = defaultApplyTimeout
	}
	if c.BackfillWindow < 0 {
		return errors.New("backfillWindow must be >= 0")
	}
	if c.BytesInFlight == 0 {
		c.BytesInFlight = defaultBytesInFlight
	}
	if c.FanShards == 0 {
		c.FanShards = defaultFanShards
	}
	if c.ForeignKeysEnabled && c.Immediate {
		return errors.New("foreign-key mode incompatible with immediate mode")
	}
	if c.LoopName == "" {
		return errors.New("replication loops must be named")
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = defaultRetryDelay
	}
	if c.StagingDB.IsEmpty() {
		return errors.New("no staging database specified")
	}
	if c.StandbyTimeout == 0 {
		c.StandbyTimeout = defaultStandbyTimeout
	}
	if c.TargetConn == "" {
		return errors.New("no target connection string specified")
	}
	if c.TargetDB.IsEmpty() {
		return errors.New("no target database specified")
	}
	if c.TargetDBConns == 0 {
		c.TargetDBConns = defaultTargetDBConns
	}
	return nil
}

// An Option can be provided to Factory.Get() to provide any final
// adjustments to the per-Loop BaseConfig.
type Option func(cfg *BaseConfig)

// WithName appends the given name to the configuration's loop name.
func WithName(name string) Option {
	return func(cfg *BaseConfig) {
		if cfg.LoopName == "" {
			cfg.LoopName = name
		} else {
			cfg.LoopName = fmt.Sprintf("%s-%s", cfg.LoopName, name)
		}
	}
}
