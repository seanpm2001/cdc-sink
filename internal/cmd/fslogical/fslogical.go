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

// Package fslogical contains a command to perform logical replication
// from Google Cloud Firestore.
package fslogical

import (
	"net"
	"net/http"

	"github.com/cockroachdb/cdc-sink/internal/source/fslogical"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Command returns the fslogical subcommand.
func Command() *cobra.Command {
	cfg := &fslogical.Config{}

	var metricsAddr string
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "start a Google Cloud Firestore logical replication feed",
		Use:   "fslogical",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if metricsAddr != "" {
				if err := metricsServer(metricsAddr); err != nil {
					return err
				}
			}

			loops, cancelLoops, err := fslogical.Start(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			// Pause any log.Exit() or log.Fatal() until the server exits.
			log.DeferExitHandler(func() {
				cancelLoops()
				for _, loop := range loops {
					<-loop.Stopped()
				}
			})
			// Wait for shutdown. The main function uses log.Exit()
			// to call the above handler.
			<-cmd.Context().Done()
			return nil
		},
	}
	cfg.Bind(cmd.Flags())
	cmd.Flags().StringVar(&metricsAddr, "metricsAddr", "",
		"a host:port to serve metrics from at /_/varz")

	return cmd
}

// metricsServer starts a trivial prometheus endpoint server which runs
// until the context is canceled.
func metricsServer(bindAddr string) error {
	mux := &http.ServeMux{}
	mux.HandleFunc("/_/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.Handle("/_/varz", promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				EnableOpenMetrics: true,
				ErrorLog:          log.StandardLogger().WithField("promhttp", "true"),
			})))
	mux.Handle("/", http.NotFoundHandler())

	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return errors.WithStack(err)
	}
	srv := &http.Server{
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}
	log.Infof("metrics server bound to %s", l.Addr())
	go srv.Serve(l)
	return nil
}
