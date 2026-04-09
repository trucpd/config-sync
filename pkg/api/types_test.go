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
	"testing"

	"github.com/GoogleContainerTools/config-sync/pkg/api/configsync/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMapRootSync(t *testing.T) {
	rs := v1beta1.RootSync{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "root-sync",
			Namespace: "config-management-system",
		},
		Status: v1beta1.RootSyncStatus{
			Status: v1beta1.Status{
				Sync: v1beta1.SyncStatus{
					Commit: "abc123",
					Errors: []v1beta1.ConfigSyncError{
						{
							Code:         "2001",
							ErrorMessage: "sync failed",
						},
					},
				},
			},
		},
	}

	got := mapRootSync("cluster-1", rs)

	if got.Kind != "RootSync" {
		t.Fatalf("expected kind RootSync, got %q", got.Kind)
	}
	if got.ClusterName != "cluster-1" {
		t.Fatalf("expected clusterName cluster-1, got %q", got.ClusterName)
	}
	if got.CommitHash != "abc123" {
		t.Fatalf("expected commit abc123, got %q", got.CommitHash)
	}
	if got.SyncStatusCode != "2001" {
		t.Fatalf("expected sync status code 2001, got %q", got.SyncStatusCode)
	}
	if len(got.Errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(got.Errors))
	}
	if got.Errors[0].Message != "sync failed" {
		t.Fatalf("expected error message 'sync failed', got %q", got.Errors[0].Message)
	}
}

func TestMapRepoSync_NoErrors(t *testing.T) {
	rs := v1beta1.RepoSync{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "repo-sync",
			Namespace: "team-a",
		},
		Status: v1beta1.RepoSyncStatus{
			Status: v1beta1.Status{
				Sync: v1beta1.SyncStatus{
					Commit: "def456",
				},
			},
		},
	}

	got := mapRepoSync("cluster-2", rs)

	if got.Kind != "RepoSync" {
		t.Fatalf("expected kind RepoSync, got %q", got.Kind)
	}
	if got.SyncStatusCode != "" {
		t.Fatalf("expected empty sync status code, got %q", got.SyncStatusCode)
	}
	if len(got.Errors) != 0 {
		t.Fatalf("expected no errors, got %d", len(got.Errors))
	}
}
