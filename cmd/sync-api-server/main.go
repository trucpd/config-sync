// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GoogleContainerTools/config-sync/pkg/api"
	"github.com/GoogleContainerTools/config-sync/pkg/k8s"
	"k8s.io/klog/v2"
)

func main() {
	var addr string
	flag.StringVar(&addr, "addr", ":8088", "HTTP listen address")
	flag.Parse()

	k8sClient, err := k8s.NewRuntimeClient()
	if err != nil {
		klog.Fatalf("failed to initialize Kubernetes client: %v", err)
	}

	server := api.NewServer(k8sClient)
	server.StartRootSyncWatch(context.Background())

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stopCh
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			klog.Errorf("HTTP server shutdown error: %v", err)
		}
	}()

	klog.Infof("sync API server listening on %s", addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		klog.Fatalf("failed to start HTTP server: %v", err)
	}
}
