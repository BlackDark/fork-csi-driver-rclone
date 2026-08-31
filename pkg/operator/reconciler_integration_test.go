//go:build integration

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestReconcileStaleMountsSkipsProtectedPods(t *testing.T) {
	provisioner := "rclone.csi.veloxpack.io"
	podUID := "11111111-2222-3333-4444-555555555555"

	objects := []runtime.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "csi-rclone-node-xyz",
				Namespace: "system",
				UID:       "11111111-2222-3333-4444-555555555555",
			},
			Spec: corev1.PodSpec{NodeName: "node-1"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "nginx",
				Namespace: "default",
				UID:       "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
					},
				}},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-data"},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data"},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: provisioner},
				},
			},
		},
	}

	client := fake.NewSimpleClientset(objects...)
	r := NewReconciler(client, "node-1", provisioner)
	r.confirmCorrupted = func(path string) (bool, string) {
		return path == "/mnt2", "test corruption"
	}

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{
		{PodUID: string(podUID), VolumeName: "data", MountPath: "/mnt"},
		{PodUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", VolumeName: "data", MountPath: "/mnt2"},
	})
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("system").Get(context.Background(), "csi-rclone-node-xyz", metav1.GetOptions{})
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("default").Get(context.Background(), "nginx", metav1.GetOptions{})
	assert.Error(t, err)
}

func TestReconcileStaleMountsRateLimited(t *testing.T) {
	provisioner := "rclone.csi.veloxpack.io"
	now := time.Now().UTC()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app",
			Namespace: "default",
			UID:       "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "app",
				UID:        "rs-app",
				Controller: boolPtr(true),
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					CSI: &corev1.CSIVolumeSource{Driver: provisioner},
				},
			}},
		},
	}

	client := fake.NewSimpleClientset(pod)
	r := NewReconciler(client, "node-1", provisioner)
	r.mu.Lock()
	r.markRecoveredLocked(pod, now.Add(-10*time.Minute))
	r.mu.Unlock()

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{
		{PodUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", VolumeName: "data", MountPath: "/mnt"},
	})
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("default").Get(context.Background(), "app", metav1.GetOptions{})
	require.NoError(t, err)
}
