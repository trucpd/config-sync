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

package k8s

import (
	"context"

	"github.com/GoogleContainerTools/config-sync/pkg/api/configsync/v1beta1"
	"github.com/GoogleContainerTools/config-sync/pkg/core"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// Client is the Kubernetes access abstraction used by the API server.
type Client interface {
	ClusterName() string
	ListRootSyncs(ctx context.Context) ([]v1beta1.RootSync, error)
	ListRepoSyncs(ctx context.Context) ([]v1beta1.RepoSync, error)
	WatchRootSyncs(ctx context.Context) (watch.Interface, error)
}

// RuntimeClient provides Kubernetes access using controller-runtime.
type RuntimeClient struct {
	clusterName string
	client      client.WithWatch
}

// NewRuntimeClient builds a new RuntimeClient from the in-cluster or local kubeconfig.
func NewRuntimeClient() (*RuntimeClient, error) {
	cfg := ctrl.GetConfigOrDie()
	watchingClient, err := client.NewWithWatch(cfg, client.Options{
		Scheme: core.Scheme,
	})
	if err != nil {
		return nil, err
	}

	return &RuntimeClient{
		clusterName: detectClusterName(),
		client:      watchingClient,
	}, nil
}

// ClusterName returns the best-effort cluster name.
func (c *RuntimeClient) ClusterName() string {
	return c.clusterName
}

// ListRootSyncs returns all RootSync objects in the cluster.
func (c *RuntimeClient) ListRootSyncs(ctx context.Context) ([]v1beta1.RootSync, error) {
	var list v1beta1.RootSyncList
	if err := c.client.List(ctx, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// ListRepoSyncs returns all RepoSync objects in the cluster.
func (c *RuntimeClient) ListRepoSyncs(ctx context.Context) ([]v1beta1.RepoSync, error) {
	var list v1beta1.RepoSyncList
	if err := c.client.List(ctx, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// WatchRootSyncs watches all RootSync objects in the cluster.
func (c *RuntimeClient) WatchRootSyncs(ctx context.Context) (watch.Interface, error) {
	return c.client.Watch(ctx, &v1beta1.RootSyncList{})
}

func detectClusterName() string {
	const unknownCluster = "unknown"

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := loadingRules.Load()
	if err != nil || cfg == nil {
		return unknownCluster
	}

	if cfg.CurrentContext == "" {
		return unknownCluster
	}

	ctx, ok := cfg.Contexts[cfg.CurrentContext]
	if !ok || ctx == nil {
		return cfg.CurrentContext
	}
	if ctx.Cluster != "" {
		return ctx.Cluster
	}
	return cfg.CurrentContext
}
