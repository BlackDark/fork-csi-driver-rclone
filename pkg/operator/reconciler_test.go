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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestShouldSkipPod(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "kube-system",
			pod:  &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "kube-system"}},
			want: true,
		},
		{
			name: "csi-rclone in name",
			pod:  &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "csi-rclone-node-abc", Namespace: "system"}},
			want: true,
		},
		{
			name: "csi daemonset owner",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "node-driver",
					Namespace: "system",
					OwnerReferences: []metav1.OwnerReference{{
						Kind: "DaemonSet",
						Name: "csi-rclone-node",
					}},
				},
			},
			want: true,
		},
		{
			name: "workload pod",
			pod:  &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nginx", Namespace: "default"}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ShouldSkipPod(tt.pod))
		})
	}
}

func TestIsRateLimited(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	cooldown := time.Hour

	podRecent := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				RecoveryAnnotation: now.Add(-30 * time.Minute).Format(time.RFC3339),
			},
		},
	}
	assert.True(t, IsRateLimited(podRecent, now, cooldown))

	podOld := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				RecoveryAnnotation: now.Add(-2 * time.Hour).Format(time.RFC3339),
			},
		},
	}
	assert.False(t, IsRateLimited(podOld, now, cooldown))

	podMissing := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{}}
	assert.False(t, IsRateLimited(podMissing, now, cooldown))

	podInvalid := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{RecoveryAnnotation: "not-a-timestamp"},
		},
	}
	assert.True(t, IsRateLimited(podInvalid, now, cooldown))
}

func TestReconcileStaleMountsLazyUmountsMissingPod(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	r.SetOrphanLazyUmount(true)

	var umounted []string
	r.lazyUmount = func(path string) error {
		umounted = append(umounted, path)
		return nil
	}

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID:     "dead-uid",
		VolumeName: "data",
		MountPath:  "/var/lib/kubelet/pods/dead-uid/volumes/kubernetes.io~csi/data/mount",
	}})
	require.NoError(t, err)
	require.Equal(t, []string{
		"/var/lib/kubelet/pods/dead-uid/volumes/kubernetes.io~csi/data/mount",
	}, umounted)
}

func TestReconcileStaleMountsSkipsLazyUmountWhenDisabled(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	r.SetOrphanLazyUmount(false)

	called := false
	r.lazyUmount = func(string) error {
		called = true
		return nil
	}

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID:     "dead-uid",
		VolumeName: "data",
		MountPath:  "/mnt/orphan",
	}})
	require.NoError(t, err)
	assert.False(t, called)
}
