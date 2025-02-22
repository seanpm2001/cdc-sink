// Code generated by Wire. DO NOT EDIT.

//go:generate go run github.com/google/wire/cmd/wire
//go:build !wireinject
// +build !wireinject

package mylogical

import (
	"context"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/staging/memo"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
)

// Injectors from injector.go:

// Start creates a MySQL/MariaDB logical replication loop using the
// provided configuration.
func Start(ctx context.Context, config *Config) (*logical.Loop, func(), error) {
	scriptConfig, err := logical.ProvideUserScriptConfig(config)
	if err != nil {
		return nil, nil, err
	}
	loader, err := script.ProvideLoader(scriptConfig)
	if err != nil {
		return nil, nil, err
	}
	baseConfig, err := logical.ProvideBaseConfig(config, loader)
	if err != nil {
		return nil, nil, err
	}
	targetPool, cleanup, err := logical.ProvideTargetPool(ctx, baseConfig)
	if err != nil {
		return nil, nil, err
	}
	stagingDB, err := logical.ProvideStagingDB(baseConfig)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	configs, cleanup2, err := apply.ProvideConfigs(ctx, targetPool, stagingDB)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	watchers, cleanup3 := schemawatch.ProvideFactory(targetPool)
	appliers, cleanup4 := apply.ProvideFactory(configs, watchers)
	stagingPool := logical.ProvideStagingPool(targetPool)
	memoMemo, err := memo.ProvideMemo(ctx, stagingPool, stagingDB)
	if err != nil {
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	targetSchema := logical.ProvideUserScriptTarget(baseConfig)
	userScript, err := script.ProvideUserScript(ctx, configs, loader, stagingPool, targetSchema, watchers)
	if err != nil {
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	factory, cleanup5 := logical.ProvideFactory(appliers, config, memoMemo, stagingPool, targetPool, watchers, userScript)
	dialect, err := ProvideDialect(config)
	if err != nil {
		cleanup5()
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	loop, err := logical.ProvideLoop(ctx, factory, dialect)
	if err != nil {
		cleanup5()
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	return loop, func() {
		cleanup5()
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
	}, nil
}
