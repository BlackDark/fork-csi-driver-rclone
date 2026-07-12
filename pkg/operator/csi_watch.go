/*
Copyright 2025 Veloxpack.io

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package operator

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const defaultCSINodeLabelSelector = "app=csi-rclone-node"

// DefaultCSINodeLabelSelector returns the default label selector for CSI node pods.
func DefaultCSINodeLabelSelector() string {
	return defaultCSINodeLabelSelector
}

// CSINodeTracker detects when the local CSI node pod restarts.
type CSINodeTracker struct {
	client        kubernetes.Interface
	nodeName      string
	labelSelector string
	lastUID       string
	initialized   bool
}

// NewCSINodeTracker builds a tracker for CSI node pod restarts on this node.
func NewCSINodeTracker(client kubernetes.Interface, nodeName, labelSelector string) *CSINodeTracker {
	if labelSelector == "" {
		labelSelector = defaultCSINodeLabelSelector
	}
	return &CSINodeTracker{
		client:        client,
		nodeName:      nodeName,
		labelSelector: labelSelector,
	}
}

// CheckRestarted reports whether the CSI node pod UID changed since the last check.
// The first successful observation seeds state and does not count as a restart.
func (t *CSINodeTracker) CheckRestarted(ctx context.Context) (bool, error) {
	uid, err := t.currentCSINodeUID(ctx)
	if err != nil {
		return false, err
	}
	if uid == "" {
		return false, nil
	}

	if !t.initialized {
		t.lastUID = uid
		t.initialized = true
		return false, nil
	}

	if uid == t.lastUID {
		return false, nil
	}

	klog.InfoS("CSI node pod restart detected", "node", t.nodeName, "previousUID", t.lastUID, "currentUID", uid)
	t.lastUID = uid
	return true, nil
}

func (t *CSINodeTracker) currentCSINodeUID(ctx context.Context) (string, error) {
	pods, err := t.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + t.nodeName,
		LabelSelector: t.labelSelector,
	})
	if err != nil {
		return "", fmt.Errorf("list CSI node pods on %s: %w", t.nodeName, err)
	}

	var chosen *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if chosen == nil || pod.CreationTimestamp.After(chosen.CreationTimestamp.Time) {
			chosen = pod
		}
	}
	if chosen == nil {
		return "", nil
	}
	return string(chosen.UID), nil
}
