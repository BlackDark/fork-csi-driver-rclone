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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	defaultCSINodeLabelSelector = "app=csi-rclone-node"
	defaultCSIReadyPollInterval = time.Second
	defaultCSIReadyTimeout      = 90 * time.Second
)

// DefaultCSINodeLabelSelector returns the default label selector for CSI node pods.
func DefaultCSINodeLabelSelector() string {
	return defaultCSINodeLabelSelector
}

// DefaultCSIRestartReadyTimeout returns the default wait for CSI node Ready after a restart.
func DefaultCSIRestartReadyTimeout() time.Duration {
	return defaultCSIReadyTimeout
}

// CSINodeTracker detects when the local CSI node pod restarts.
type CSINodeTracker struct {
	client        kubernetes.Interface
	nodeName      string
	labelSelector string
	lastUID       string
	initialized   bool
	readyPoll     time.Duration // overridable in tests; zero uses defaultCSIReadyPollInterval
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
	pod, err := t.newestCSINodePod(ctx)
	if err != nil {
		return false, err
	}
	if pod == nil {
		return false, nil
	}
	uid := string(pod.UID)

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

// WaitUntilReady waits until the newest CSI node pod on this node is Ready.
// On timeout it logs a warning and returns nil so workload recovery can proceed.
// Context cancellation returns ctx.Err() without proceeding.
func (t *CSINodeTracker) WaitUntilReady(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultCSIReadyTimeout
	}
	poll := t.readyPoll
	if poll <= 0 {
		poll = defaultCSIReadyPollInterval
	}

	klog.InfoS("waiting for CSI node pod Ready", "node", t.nodeName, "timeout", timeout)
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	for {
		pod, err := t.newestCSINodePod(ctx)
		if err != nil {
			klog.V(4).InfoS("CSI node Ready check list failed", "node", t.nodeName, "err", err)
		} else if pod != nil && isPodReady(pod) {
			klog.InfoS("CSI node pod is Ready", "node", t.nodeName, "pod", pod.Name, "uid", pod.UID)
			return nil
		}

		if time.Now().After(deadline) {
			klog.InfoS("timed out waiting for CSI node pod Ready; proceeding with workload recovery",
				"node", t.nodeName, "timeout", timeout)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (t *CSINodeTracker) newestCSINodePod(ctx context.Context) (*corev1.Pod, error) {
	pods, err := t.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + t.nodeName,
		LabelSelector: t.labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("list CSI node pods on %s: %w", t.nodeName, err)
	}

	var chosen *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if chosen == nil || pod.CreationTimestamp.After(chosen.CreationTimestamp.Time) {
			chosen = pod
		}
	}
	return chosen, nil
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
