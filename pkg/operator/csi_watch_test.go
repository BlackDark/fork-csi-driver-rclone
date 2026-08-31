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
	"k8s.io/apimachinery/pkg/types"
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

func TestWaitUntilReadyWhenAlreadyReady(t *testing.T) {
	pod := readyCSINodePod("csi-rclone-node-abc", "uid-1", true)
	client := fake.NewSimpleClientset(pod)
	tracker := NewCSINodeTracker(client, "node-1", "app=csi-rclone-node")
	tracker.readyPoll = 10 * time.Millisecond

	err := tracker.WaitUntilReady(context.Background(), time.Second)
	require.NoError(t, err)
}

func TestWaitUntilReadySucceedsAfterBecomingReady(t *testing.T) {
	pod := readyCSINodePod("csi-rclone-node-abc", "uid-1", false)
	client := fake.NewSimpleClientset(pod)
	tracker := NewCSINodeTracker(client, "node-1", "app=csi-rclone-node")
	tracker.readyPoll = 10 * time.Millisecond

	go func() {
		time.Sleep(40 * time.Millisecond)
		updated := readyCSINodePod("csi-rclone-node-abc", "uid-1", true)
		updated.ResourceVersion = "2"
		_, err := client.CoreV1().Pods("system").Update(context.Background(), updated, metav1.UpdateOptions{})
		require.NoError(t, err)
	}()

	err := tracker.WaitUntilReady(context.Background(), time.Second)
	require.NoError(t, err)
}

func TestWaitUntilReadyTimeoutProceeds(t *testing.T) {
	pod := readyCSINodePod("csi-rclone-node-abc", "uid-1", false)
	client := fake.NewSimpleClientset(pod)
	tracker := NewCSINodeTracker(client, "node-1", "app=csi-rclone-node")
	tracker.readyPoll = 10 * time.Millisecond

	start := time.Now()
	err := tracker.WaitUntilReady(context.Background(), 50*time.Millisecond)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

func TestWaitUntilReadyContextCancel(t *testing.T) {
	pod := readyCSINodePod("csi-rclone-node-abc", "uid-1", false)
	client := fake.NewSimpleClientset(pod)
	tracker := NewCSINodeTracker(client, "node-1", "app=csi-rclone-node")
	tracker.readyPoll = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err := tracker.WaitUntilReady(ctx, time.Second)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIsPodReady(t *testing.T) {
	assert.False(t, isPodReady(nil))
	assert.False(t, isPodReady(readyCSINodePod("p", "u", false)))
	assert.True(t, isPodReady(readyCSINodePod("p", "u", true)))
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

func readyCSINodePod(name, uid string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "system",
			UID:               types.UID(uid),
			Labels:            map[string]string{"app": "csi-rclone-node"},
			CreationTimestamp: metav1.Now(),
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: status,
			}},
		},
	}
}
