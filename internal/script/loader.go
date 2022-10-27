// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package script

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/dop251/goja"
	esbuild "github.com/evanw/esbuild/pkg/api"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A JS function to dispatch source documents onto target tables.
//
// Look on my Works, ye Mighty, and despair!
//
//	{ doc } => { "target" : [ { doc }, ... ], "otherTarget" : [ { doc }, ... ], ... }
type dispatchJS func(
	doc map[string]any,
	meta map[string]any,
) (map[string][]map[string]any, error)

// A simple mapping function.
//
//	{ doc } => { doc }
type mapJS func(
	doc map[string]any,
	meta map[string]any,
) (map[string]any, error)

// sourceJS is used in the API binding.
type sourceJS struct {
	DeletesTo string     `goja:"deletesTo"`
	Dispatch  dispatchJS `goja:"dispatch"`
	Recurse   bool       `goja:"recurse"`
	Target    string     `goja:"target"`
}

// targetJS is used in the API binding. The apply.Config.SourceNames
// field is ignored, since that can be taken care of by the Map
// function.
type targetJS struct {
	// Column names.
	CASColumns []string `goja:"cas"`
	// Column to duration.
	Deadlines map[string]string `goja:"deadlines"`
	// Column to SQL expression to pass through.
	Exprs map[string]string `goja:"exprs"`
	// Column name.
	Extras string `goja:"extras"`
	// Column names.
	Ignore map[string]bool `goja:"ignore"`
	// Mutation to mutation.
	Map mapJS `goja:"map"`
}

// Loader is responsible for the first-pass execution of the user
// script. It will load all required resources, parse, and execute the
// top-level API calls.
type Loader struct {
	fs           fs.FS                 // Used by require.
	options      Options               // Target of api.setOptions().
	requireStack []*url.URL            // Allows relative import paths.
	requireCache map[string]goja.Value // Keys are URLs.
	rt           *goja.Runtime         // JS Runtime.
	sources      map[string]*sourceJS  // User configuration.
	targets      map[string]*targetJS  // User configuration.
}

// configureSource is exported to the JS runtime.
func (l *Loader) configureSource(sourceName string, bag *sourceJS) error {
	if (bag.Dispatch != nil) == (bag.Target != "") {
		return errors.Errorf("configureSource(%q): one of mapper or target must be set", sourceName)
	}
	l.sources[sourceName] = bag
	return nil

}

// configureTable is exported to the JS runtime.
func (l *Loader) configureTable(tableName string, bag *targetJS) error {
	l.targets[tableName] = bag
	return nil
}

// require is exported to the JS runtime and implements a basic version
// of the NodeJS-style require() function. The referenced module
// contents are loaded, converted to ES5 in CommonJS packaging, and then
// executed.
func (l *Loader) require(module string) (goja.Value, error) {
	// Look for an exact-match (e.g. the API import).
	if found, ok := l.requireCache[module]; ok {
		return found, nil
	}

	// The required path is parsed as a URL, relative to the top of the
	// require stack.  This allows, for example, a script to be loaded
	// from an external source which then refers to sibling paths.
	var err error
	var source *url.URL
	if len(l.requireStack) == 0 {
		// We bootstrap the runtime with require("file:///<main.js>")
		source, err = url.Parse(module)
	} else {
		parent := l.requireStack[len(l.requireStack)-1]
		source, err = parent.Parse(module)
		// This is a bit of a hack for .ts files, since their import
		// strings don't generally include the .ts extension.
		if path.Ext(parent.Path) == ".ts" && path.Ext(source.Path) == "" {
			source.Path += ".ts"
		}
	}
	if err != nil {
		return nil, err
	}

	// At this point, the source is an absolute URL, so we'll use it
	// as the key.  We perform a second lookup to see if the external
	// module has been previously required.
	key := source.String()
	if found, ok := l.requireCache[key]; ok {
		return found, nil
	}

	// Push the script's location onto the stack, pop when we're done.
	l.requireStack = append(l.requireStack, source)
	defer func() { l.requireStack = l.requireStack[:len(l.requireStack)-1] }()

	log.Debugf("loading user script %s", source)

	// Acquire the contents of the script.  A file:// URL is loaded from
	// the supplied fs.FS, while http(s):// makes the relevant request.
	var data []byte
	switch source.Scheme {
	case "file":
		f, err := l.fs.Open(source.Path[1:])
		if err != nil {
			return nil, errors.Wrap(err, source.Path)
		}
		defer f.Close()
		data, err = io.ReadAll(f)
		if err != nil {
			return nil, errors.Wrap(err, source.Path)
		}

	case "http", "https":
		resp, err := http.Get(source.String())
		if err != nil {
			return nil, err
		}
		data, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported scheme %s", source.Scheme)
	}

	// These options will create a self-executing closure that provides
	// the expected ambient symbols for a CommonJS script. The header
	// assigns a stub object to the global __require_cache map to defuse
	// any cyclical module references. It then replaces that stub object
	// with the evaluated module exports to support resource imports.
	opts := esbuild.TransformOptions{
		Banner: fmt.Sprintf(`
__require_cache[%[1]q]=(()=>{
var exports = __require_cache[%[1]q] = {};
var module = {exports: exports};`, key),
		Footer:     "return module.exports;})()",
		Format:     esbuild.FormatCommonJS,
		Loader:     esbuild.LoaderDefault,
		Sourcefile: key,
		Target:     esbuild.ES2015,
	}
	// Source maps improve error messages from the JS runtime.
	if strings.HasSuffix(key, ".js") || strings.HasSuffix(key, ".ts") {
		opts.Sourcemap = esbuild.SourceMapInline
	}

	// Process the script or resource into the equivalent JS source.
	res := esbuild.Transform(string(data), opts)
	if len(res.Errors) > 0 {
		strs := esbuild.FormatMessages(res.Errors, esbuild.FormatMessagesOptions{TerminalWidth: 80})
		for _, str := range strs {
			log.Error(str)
		}
		return nil, errors.New("could not transform source, see log messages for details")
	}

	// Compile the source.
	prog, err := goja.Compile(key, string(res.Code), true)
	if err != nil {
		return nil, err
	}

	// Execute the program, which returns the module's exports. Note
	// that the assigment to l.requireCache happens via the
	// __require_cache binding in the script prelude.
	return l.rt.RunProgram(prog)
}

// setOptions is an escape-hatch for configuring dialects at runtime.
func (l *Loader) setOptions(data map[string]string) error {
	for k, v := range data {
		if err := l.options.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}
