// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"embed"
	"fmt"
	"strings"
	"sync"
	"text/template"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"
)

var (
	//go:embed queries/*.tmpl
	queries embed.FS

	parsed = template.Must(template.New("").Funcs(template.FuncMap{
		"isUDT": func(x interface{}) bool {
			_, ok := x.(ident.UDT)
			return ok
		},
		// nl is a hack to force the inclusion of newlines in the output
		// since we generally use the whitespace-consuming template
		// syntax.
		"nl": func() string { return "\n" },

		// qualify is used with the "join" template to emit a list of
		// qualified database identifiers. The prefix is prepended to
		// each column's name using a dot separator.
		//   {{ range $name := qualify "foo" $.Columns }}
		// would return values such as
		//     foo.PK, foo.Val0, foo.Val1, ....
		"qualify": func(prefix interface{}, cols []types.ColData) ([]string, error) {
			var id ident.Ident
			switch t := prefix.(type) {
			case string:
				id = ident.New(t)
			case ident.Ident:
				id = t
			case ident.Table:
				id = t.Table()
			default:
				return nil, errors.Errorf("unsupported conversion %T", t)
			}
			ret := make([]string, len(cols))
			for i := range ret {
				ret[i] = fmt.Sprintf("%s.%s", id, cols[i].Name)
			}
			return ret, nil
		},
	}).ParseFS(queries, "**/*.tmpl"))
	deleteTemplate = parsed.Lookup("delete.tmpl")
	upsertTemplate = parsed.Lookup("upsert.tmpl")
)

// A templateCache stores variations of the delete and upsert commands
// at varying batch sizes. The map keys are just ints representing the
// number of rows that the statement should process.
type templateCache struct {
	sync.Mutex
	deletes *lru.Cache
	upserts *lru.Cache
}

type templates struct {
	Columns    []types.ColData        // All non-ignored columns; Keys + Columns
	Conditions []types.ColData        // The version-like fields for CAS ops.
	Deadlines  types.Deadlines        // Allow too-old data to just be dropped.
	Exprs      map[ident.Ident]string // Value-replacement expressions.
	Keys       []types.ColData        // Primary-key or UNIQUE-index columns acting as a key.
	NonKeys    []types.ColData        // Columns not part of the keys.
	TableName  ident.Table            // The target table.
	UseUpsert  bool                   // Toggle between UPSERT and IOCDU.
	cache      *templateCache         // Memoize calls to delete() and upsert().

	// The variables below here are updated during evaluation.

	RowCount int // The number of rows to be applied.
}

// newTemplates constructs a new templates instance, performing some
// pre-computations to identify primary keys and to filter out ignored
// columns.
func newTemplates(target ident.Table, cfgData *Config, colData []types.ColData) *templates {
	altMap := make(map[ident.Ident]int, len(cfgData.AltKeys))
	for idx, name := range cfgData.AltKeys {
		altMap[name] = idx
	}
	useAltKeys := len(cfgData.AltKeys) > 0
	// Map cas column names to their order in the comparison tuple.
	casMap := make(map[ident.Ident]int, len(cfgData.CASColumns))
	for idx, name := range cfgData.CASColumns {
		casMap[name] = idx
	}

	ret := &templates{
		Conditions: make([]types.ColData, len(cfgData.CASColumns)),
		Columns:    append([]types.ColData(nil), colData...),
		Deadlines:  cfgData.Deadlines,
		Exprs:      cfgData.Exprs,
		TableName:  target,
		UseUpsert:  len(cfgData.AltKeys) == 0,
		cache: &templateCache{
			deletes: lru.New(batches.Size() / 10),
			upserts: lru.New(batches.Size() / 10),
		},
	}

	// Build the list of key and data columns.
	for _, col := range ret.Columns {
		if col.Ignored || cfgData.Ignore[col.Name] {
			continue
		}

		// Determine if the column is used as a key. This will either be
		// part of the primary key of the target table, or columns from
		// another UNIQUE index.
		var treatAsKey bool
		if useAltKeys {
			if _, isAltKey := altMap[col.Name]; isAltKey {
				treatAsKey = true
			} else if col.Primary {
				// We ignore the table's primary keys. We have required
				// elsewhere that when alt-keys are used, all PK columns
				// must have a DEFAULT value.
				continue
			}
		} else {
			treatAsKey = col.Primary
		}

		if treatAsKey {
			ret.Keys = append(ret.Keys, col)
		} else {
			ret.NonKeys = append(ret.NonKeys, col)
		}
		if idx, isCas := casMap[col.Name]; isCas {
			ret.Conditions[idx] = col
		}
	}

	ret.Columns = make([]types.ColData, len(ret.Keys)+len(ret.NonKeys))
	copy(ret.Columns, ret.Keys)
	copy(ret.Columns[len(ret.Keys):], ret.NonKeys)

	return ret
}

// varPair is returned by Vars, to associate a Column with a
// substitution-parameter index.
type varPair struct {
	Column types.ColData
	// If non-empty, a user-configured SQL expression for the value.
	// The Index substitution parameter will have been injected into
	// the expressed.
	Expr string
	// The 1-based SQL substitution parameter index. A zero value
	// indicates that the varPair has a constant expression which
	// does not depend on an injected value.
	Index int
}

// Vars is a generator function that returns windows of 1-based
// substitution parameters for the given columns. These are used to
// generate the multi-VALUES ($1,$2, ...), ($55, $56) clauses.
func (t *templates) Vars() [][]varPair {
	ret := make([][]varPair, t.RowCount)
	pairIdx := 1
	for row := range ret {
		ret[row] = make([]varPair, len(t.Columns))
		for colIdx, col := range t.Columns {
			vp := varPair{
				Column: col,
				Index:  pairIdx,
			}
			pairIdx++

			if pattern, ok := t.Exprs[col.Name]; ok {
				vp.Expr = strings.ReplaceAll(
					pattern, substitutionToken, fmt.Sprintf("$%d", vp.Index))
				// A constant expression doesn't occupy an index slot.
				if vp.Expr == pattern {
					vp.Index = 0
					pairIdx--
				}
			}

			ret[row][colIdx] = vp
		}
	}
	return ret
}

func (t *templates) delete(rowCount int) (string, error) {
	t.cache.Lock()
	defer t.cache.Unlock()
	if found, ok := t.cache.deletes.Get(rowCount); ok {
		applyTemplateHits.WithLabelValues("delete").Inc()
		return found.(string), nil
	}
	applyTemplateMisses.WithLabelValues("delete").Inc()

	// Make a copy that we can tweak.
	cpy := *t
	cpy.Columns = t.Keys // XXX need alt keys
	cpy.RowCount = rowCount

	var buf strings.Builder
	err := deleteTemplate.Execute(&buf, &cpy)
	ret, err := buf.String(), errors.WithStack(err)
	if err == nil {
		t.cache.deletes.Add(rowCount, ret)
	}
	return ret, err
}

func (t *templates) upsert(rowCount int) (string, error) {
	t.cache.Lock()
	defer t.cache.Unlock()
	if found, ok := t.cache.upserts.Get(rowCount); ok {
		applyTemplateHits.WithLabelValues("upsert").Inc()
		return found.(string), nil
	}
	applyTemplateMisses.WithLabelValues("upsert").Inc()

	// Make a copy that we can tweak.
	cpy := *t
	cpy.RowCount = rowCount

	var buf strings.Builder
	err := upsertTemplate.Execute(&buf, &cpy)
	ret, err := buf.String(), errors.WithStack(err)
	if err == nil {
		t.cache.upserts.Add(rowCount, ret)
	}
	return ret, err
}
