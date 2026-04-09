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

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/GoogleContainerTools/config-sync/pkg/api/configsync/v1beta1"
	"github.com/GoogleContainerTools/config-sync/pkg/k8s"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

// Server exposes sync status APIs for UI clients.
type Server struct {
	k8sClient   k8s.Client
	broadcaster *eventBroadcaster
}

// NewServer creates a new API server.
func NewServer(k8sClient k8s.Client) *Server {
	return &Server{
		k8sClient:   k8sClient,
		broadcaster: newEventBroadcaster(),
	}
}

// Handler returns the HTTP handler for this API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/sync-objects", s.handleListSyncObjects)
	mux.HandleFunc("/api/v1/events/rootsync", s.handleRootSyncSSE)
	return mux
}

// StartRootSyncWatch starts the RootSync watch loop and publishes SSE events.
func (s *Server) StartRootSyncWatch(ctx context.Context) {
	go s.watchRootSyncEvents(ctx)
}

func (s *Server) handleListSyncObjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	ctx := r.Context()

	rootSyncs, err := s.k8sClient.ListRootSyncs(ctx)
	if err != nil {
		writeServerError(w, err)
		return
	}

	repoSyncs, err := s.k8sClient.ListRepoSyncs(ctx)
	if err != nil {
		writeServerError(w, err)
		return
	}

	clusterName := s.k8sClient.ClusterName()
	response := ListSyncObjectsResponse{
		Items: make([]SyncObjectDTO, 0, len(rootSyncs)+len(repoSyncs)),
	}
	for _, rootSync := range rootSyncs {
		response.Items = append(response.Items, mapRootSync(clusterName, rootSync))
	}
	for _, repoSync := range repoSyncs {
		response.Items = append(response.Items, mapRepoSync(clusterName, repoSync))
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRootSyncSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeServerError(w, errors.New("streaming is not supported"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	eventCh := s.broadcaster.subscribe()
	defer s.broadcaster.unsubscribe(eventCh)

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-eventCh:
			payload, err := json.Marshal(event)
			if err != nil {
				klog.Errorf("failed to marshal SSE payload: %v", err)
				continue
			}

			if _, err := fmt.Fprintf(w, "event: rootsync\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) watchRootSyncEvents(ctx context.Context) {
	const watchRetryDelay = 2 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		watcher, err := s.k8sClient.WatchRootSyncs(ctx)
		if err != nil {
			klog.Errorf("failed to watch RootSync objects: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(watchRetryDelay):
			}
			continue
		}

		clusterName := s.k8sClient.ClusterName()
		resultCh := watcher.ResultChan()
		closed := false
		for !closed {
			select {
			case <-ctx.Done():
				watcher.Stop()
				return
			case event, ok := <-resultCh:
				if !ok {
					watcher.Stop()
					closed = true
					continue
				}
				rootSync, ok := event.Object.(*v1beta1.RootSync)
				if !ok || rootSync == nil {
					continue
				}
				s.broadcaster.broadcast(RootSyncEvent{
					Type:   watchEventType(event.Type),
					Object: mapRootSync(clusterName, *rootSync),
				})
			}
		}
	}
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func writeServerError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]string{
		"error": err.Error(),
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		klog.Errorf("failed to encode JSON response: %v", err)
	}
}

func watchEventType(eventType watch.EventType) string {
	switch eventType {
	case watch.Added:
		return "ADDED"
	case watch.Modified:
		return "MODIFIED"
	case watch.Deleted:
		return "DELETED"
	default:
		return "UNKNOWN"
	}
}

type eventBroadcaster struct {
	mutex       sync.RWMutex
	subscribers map[chan RootSyncEvent]struct{}
}

func newEventBroadcaster() *eventBroadcaster {
	return &eventBroadcaster{
		subscribers: make(map[chan RootSyncEvent]struct{}),
	}
}

func (b *eventBroadcaster) subscribe() chan RootSyncEvent {
	eventCh := make(chan RootSyncEvent, 16)
	b.mutex.Lock()
	b.subscribers[eventCh] = struct{}{}
	b.mutex.Unlock()
	return eventCh
}

func (b *eventBroadcaster) unsubscribe(eventCh chan RootSyncEvent) {
	b.mutex.Lock()
	delete(b.subscribers, eventCh)
	close(eventCh)
	b.mutex.Unlock()
}

func (b *eventBroadcaster) broadcast(event RootSyncEvent) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	for sub := range b.subscribers {
		select {
		case sub <- event:
		default:
			klog.Warningf("dropping RootSync SSE event for slow subscriber")
		}
	}
}
