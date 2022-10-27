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
	"os"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config adds dialect-specific configuration to the core logical loop.
type Config struct {
	logical.BaseConfig
	// The number of documents to load at once during a backfill operation.
	BackfillBatchSize int
	// A JSON service-account key for the Firestore API.
	CredentialsFile string
	// Copies the document id from the doc metadata into the mutation
	// using this property name.
	DocumentIDProperty ident.Ident
	// The Firebase project id. Usually inferred from the credentials.
	ProjectID string
	// The name of a collection that contains tombstones for documents
	// that were deleted while cdc-sink is offline.
	TombstoneCollection string
	// The name of the document property within TombstoneCollection
	// that stores the name of the
	TombstoneCollectionProperty ident.Ident
	// By default, the tombstone mapper will reject any tombstone
	// documents that cannot be mapped onto a target table. Setting this
	// property to true will ignore unmapped tombstones.
	TombstoneIgnoreUnmapped bool
	// The name of a document property used for high-water marks.
	UpdatedAtProperty ident.Ident
}

// Bind adds flags to the pflag.FlagSet to populate the Config.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.BaseConfig.Bind(f)

	// Always opt into backfilling, since we never have transactional
	// boundaries to contend with. Values assigned in Preflight()
	f.Lookup("backfillWindow").Hidden = true
	f.Lookup("immediate").Hidden = true

	f.IntVar(&c.BackfillBatchSize, "backfillBatchSize", 10_000,
		"the number of documents to load when backfilling")
	f.StringVar(&c.CredentialsFile, "credentials", "",
		"a file containing JSON service credentials.")
	// NB: Keep default value in sync with doc on tombstones.
	f.Var(ident.NewValue("id", &c.DocumentIDProperty), "docID",
		"the column name (likely the primary key) to populate with the document id")
	f.StringVar(&c.LoopName, "loopName", "fslogical",
		"identifies the logical replication loops in metrics")
	f.StringVar(&c.ProjectID, "projectID", "",
		"override the project id contained in the credentials file")
	f.StringVar(&c.TombstoneCollection, "tombstoneCollection", "",
		"the name of a collection that contains document Tombstones")
	// NB: Keep default value in sync with doc on tombstones.
	f.Var(ident.NewValue("collection", &c.TombstoneCollectionProperty),
		"tombstoneCollectionProperty",
		"the property name in a tombstone document that contains the original collection name")
	f.BoolVar(&c.TombstoneIgnoreUnmapped, "tombstoneIgnoreUnmapped", false,
		"skip, rather than reject, any tombstone documents that do not map to a target table")
	// NB: Keep default value in sync with doc on tombstones.
	f.Var(ident.NewValue("updated_at", &c.UpdatedAtProperty), "updatedAt",
		"the name of a document property used for high-water marks")
}

// Preflight adds additional checks to the base logical.Config.
func (c *Config) Preflight() error {
	if err := c.BaseConfig.Preflight(); err != nil {
		return err
	}

	c.BackfillWindow = time.Minute
	c.Immediate = true

	if c.BackfillBatchSize < 1 {
		return errors.New("backfill batch size must be >= 1")
	}

	// Only require credentials if there's no emulator.
	if os.Getenv(emulatorEnv) == "" {
		if c.CredentialsFile == "" {
			return errors.New("no credentials file specified")
		}
		if _, err := os.Stat(c.CredentialsFile); err != nil {
			return errors.Errorf("could not stat %s", c.CredentialsFile)
		}
	}

	// Require a property to store the underlying doc id in.
	if c.DocumentIDProperty.IsEmpty() {
		return errors.New("no document id property was configured")
	}

	if c.TombstoneCollection != "" {
		if c.TombstoneCollectionProperty.IsEmpty() {
			return errors.New("if Tombstones are enabled, a collection property name must be set")
		}
	}

	if c.UpdatedAtProperty.IsEmpty() {
		return errors.New("no updated_at property name given")
	}

	return nil
}
