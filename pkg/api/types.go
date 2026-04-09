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
	"github.com/GoogleContainerTools/config-sync/pkg/api/configsync/v1beta1"
)

// SyncErrorDTO is a UI-facing view of a Config Sync error.
type SyncErrorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// SyncObjectDTO is a UI-facing summary for RootSync/RepoSync resources.
type SyncObjectDTO struct {
	Kind           string         `json:"kind"`
	Name           string         `json:"name"`
	Namespace      string         `json:"namespace"`
	ClusterName    string         `json:"clusterName"`
	SyncStatusCode string         `json:"syncStatusCode"`
	CommitHash     string         `json:"commitHash"`
	Errors         []SyncErrorDTO `json:"errors"`
}

// ListSyncObjectsResponse is the REST response payload for listing all sync objects.
type ListSyncObjectsResponse struct {
	Items []SyncObjectDTO `json:"items"`
}

// RootSyncEvent is streamed over SSE for RootSync watch updates.
type RootSyncEvent struct {
	Type   string        `json:"type"`
	Object SyncObjectDTO `json:"object"`
}

func mapRootSync(clusterName string, rs v1beta1.RootSync) SyncObjectDTO {
	return SyncObjectDTO{
		Kind:           "RootSync",
		Name:           rs.Name,
		Namespace:      rs.Namespace,
		ClusterName:    clusterName,
		SyncStatusCode: syncStatusCode(rs.Status.Sync),
		CommitHash:     rs.Status.Sync.Commit,
		Errors:         mapErrors(rs.Status.Sync.Errors),
	}
}

func mapRepoSync(clusterName string, rs v1beta1.RepoSync) SyncObjectDTO {
	return SyncObjectDTO{
		Kind:           "RepoSync",
		Name:           rs.Name,
		Namespace:      rs.Namespace,
		ClusterName:    clusterName,
		SyncStatusCode: syncStatusCode(rs.Status.Sync),
		CommitHash:     rs.Status.Sync.Commit,
		Errors:         mapErrors(rs.Status.Sync.Errors),
	}
}

func mapErrors(errors []v1beta1.ConfigSyncError) []SyncErrorDTO {
	results := make([]SyncErrorDTO, 0, len(errors))
	for _, syncErr := range errors {
		results = append(results, SyncErrorDTO{
			Code:    syncErr.Code,
			Message: syncErr.ErrorMessage,
		})
	}
	return results
}

func syncStatusCode(syncStatus v1beta1.SyncStatus) string {
	if len(syncStatus.Errors) > 0 {
		return syncStatus.Errors[0].Code
	}
	return ""
}
