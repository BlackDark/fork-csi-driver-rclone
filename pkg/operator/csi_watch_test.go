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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCSINodeTrackerSeedsWithoutRestart(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "csi-rclone-node-abc",
			Namespace: "system",
			UID:       "uid-1",
			Labels:    map[string]string{"app": "csi-rclone-node"},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
	client := fake.NewSimpleClientset(pod)
	tracker := NewCSINodeTracker(client, "node-1", "app=csi-rclone-node")

	restarted, err := tracker.CheckRestarted(context.Background())
	require.NoError(t, err)
	assert.False(t, restarted)
}

func TestCSINodeTrackerDetectsRestart(t *testing.T) {
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "csi-rclone-node-abc",
			Namespace:         "system",
			UID:               "uid-1",
			Labels:            map[string]string{"app": "csi-rclone-node"},
			CreationTimestamp: metav1.Now(),
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
	client := fake.NewSimpleClientset(oldPod)
	tracker := NewCSINodeTracker(client, "node-1", "app=csi-rclone-node")

	_, err := tracker.CheckRestarted(context.Background())
	require.NoError(t, err)

	newPod := oldPod.DeepCopy()
	newPod.UID = "uid-2"
	newPod.CreationTimestamp = metav1.Now()
	newPod.ResourceVersion = "2"
	client = fake.NewSimpleClientset(newPod)
	tracker.client = client

	restarted, err := tracker.CheckRestarted(context.Background())
	require.NoError(t, err)
	assert.True(t, restarted)
}

func TestReconcileWorkloadPodsAfterCSIRestart(t *testing.T) {
	provisioner := "rclone.csi.veloxpack.io"
	objects := []runtime.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "csi-rclone-node-abc",
				Namespace: "system",
				UID:       "csi-uid",
				Labels:    map[string]string{"app": "csi-rclone-node"},
			},
			Spec: corev1.PodSpec{NodeName: "node-1"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "writer",
				Namespace: "default",
				UID:       "workload-uid",
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
	err := r.ReconcileWorkloadPodsAfterCSIRestart(context.Background())
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("default").Get(context.Background(), "writer", metav1.GetOptions{})
	assert.Error(t, err)
	_, err = client.CoreV1().Pods("system").Get(context.Background(), "csi-rclone-node-abc", metav1.GetOptions{})
	require.NoError(t, err)
}
