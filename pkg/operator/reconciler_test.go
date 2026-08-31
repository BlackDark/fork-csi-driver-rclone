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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
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
	r := NewReconciler(fake.NewSimpleClientset(), "node-1", "rclone.csi.veloxpack.io")
	r.cooldown = time.Hour

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "writer-abc",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "writer",
				UID:        "rs-1",
				Controller: boolPtr(true),
			}},
		},
	}
	assert.False(t, r.isRateLimited(context.Background(), pod, now))

	r.mu.Lock()
	r.markRecoveredLocked(pod, now.Add(-30*time.Minute))
	r.mu.Unlock()
	assert.True(t, r.isRateLimited(context.Background(), pod, now))

	// Same controller, different pod name still rate-limited.
	pod2 := pod.DeepCopy()
	pod2.Name = "writer-xyz"
	assert.True(t, r.isRateLimited(context.Background(), pod2, now))

	assert.False(t, r.isRateLimited(context.Background(), pod, now.Add(2*time.Hour)))
}

func TestPruneExpiredRecoveries(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	r := NewReconciler(fake.NewSimpleClientset(), "node-1", "rclone.csi.veloxpack.io")
	r.cooldown = time.Hour
	r.lastRecovery = map[string]time.Time{
		"default/Pod/expired": now.Add(-time.Hour),
		"default/Pod/recent":  now.Add(-time.Minute),
	}

	r.pruneExpiredRecoveries(now)
	assert.Equal(t, map[string]time.Time{"default/Pod/recent": now.Add(-time.Minute)}, r.lastRecovery)
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

func TestReconcileStaleMountsAbortAndKillOnOrphan(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	r.SetOrphanLazyUmount(true)
	r.SetOrphanFuseAbort(true)
	r.SetOrphanKillMountProcess(true)

	orphanPath := "/var/lib/kubelet/pods/dead-uid/volumes/kubernetes.io~csi/data/mount"
	var (
		resolved []string
		umounted []string
		aborted  []string
		killed   []string
	)
	r.resolveFuseConnID = func(path string) (string, error) {
		resolved = append(resolved, path)
		return "47", nil
	}
	r.lazyUmount = func(path string) error {
		umounted = append(umounted, path)
		return nil
	}
	r.abortFuseConn = func(id string) error {
		aborted = append(aborted, id)
		return nil
	}
	r.killMountProcess = func(path string) error {
		killed = append(killed, path)
		return nil
	}

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID:     "dead-uid",
		VolumeName: "data",
		MountPath:  orphanPath,
	}})
	require.NoError(t, err)
	assert.Equal(t, []string{orphanPath}, resolved)
	assert.Equal(t, []string{orphanPath}, umounted)
	assert.Equal(t, []string{"47"}, aborted)
	assert.Equal(t, []string{orphanPath}, killed)
	// resolve must happen before umount (captured order via sequential appends in maybeLazyUmountOrphan)
	require.Len(t, resolved, 1)
	require.Len(t, umounted, 1)
}

func TestReconcileStaleMountsSkipsAbortKillWhenFlagsOff(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	r.SetOrphanLazyUmount(true)
	r.SetOrphanFuseAbort(false)
	r.SetOrphanKillMountProcess(false)

	abortCalled := false
	killCalled := false
	r.resolveFuseConnID = func(string) (string, error) { return "9", nil }
	r.lazyUmount = func(string) error { return nil }
	r.abortFuseConn = func(string) error { abortCalled = true; return nil }
	r.killMountProcess = func(string) error { killCalled = true; return nil }

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID: "dead-uid", VolumeName: "data", MountPath: "/mnt/orphan",
	}})
	require.NoError(t, err)
	assert.False(t, abortCalled)
	assert.False(t, killCalled)
}

func TestReconcileStaleMountsNoAbortKillForLivePod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "default", UID: "live-uid"},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					CSI: &corev1.CSIVolumeSource{Driver: "rclone.csi.veloxpack.io"},
				},
			}},
		},
	}
	client := fake.NewSimpleClientset(pod)
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	r.SetOrphanLazyUmount(true)
	r.SetOrphanFuseAbort(true)
	r.SetOrphanKillMountProcess(true)
	r.confirmCorrupted = func(string) (bool, string) { return false, "" }

	abortCalled := false
	killCalled := false
	umountCalled := false
	r.resolveFuseConnID = func(string) (string, error) { return "1", nil }
	r.lazyUmount = func(string) error { umountCalled = true; return nil }
	r.abortFuseConn = func(string) error { abortCalled = true; return nil }
	r.killMountProcess = func(string) error { killCalled = true; return nil }

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID: "live-uid", VolumeName: "data",
		MountPath: "/var/lib/kubelet/pods/live-uid/volumes/kubernetes.io~csi/data/mount",
	}})
	require.NoError(t, err)
	assert.False(t, umountCalled)
	assert.False(t, abortCalled)
	assert.False(t, killCalled)
}

func TestReconcileStaleMountsSkipsAbortWhenLookupFails(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	r.SetOrphanLazyUmount(true)
	r.SetOrphanFuseAbort(true)
	r.SetOrphanKillMountProcess(true)

	abortCalled := false
	killCalled := false
	r.resolveFuseConnID = func(string) (string, error) {
		return "", assert.AnError
	}
	r.lazyUmount = func(string) error { return nil }
	r.abortFuseConn = func(string) error { abortCalled = true; return nil }
	r.killMountProcess = func(string) error { killCalled = true; return nil }

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID: "dead-uid", VolumeName: "data", MountPath: "/mnt/orphan",
	}})
	require.NoError(t, err)
	assert.False(t, abortCalled)
	assert.True(t, killCalled)
}

func TestReconcileStaleMountsNoAbortKillWhenUmountFails(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	r.SetOrphanLazyUmount(true)
	r.SetOrphanFuseAbort(true)
	r.SetOrphanKillMountProcess(true)

	abortCalled := false
	killCalled := false
	r.resolveFuseConnID = func(string) (string, error) { return "47", nil }
	r.lazyUmount = func(string) error { return assert.AnError }
	r.abortFuseConn = func(string) error { abortCalled = true; return nil }
	r.killMountProcess = func(string) error { killCalled = true; return nil }

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID: "dead-uid", VolumeName: "data", MountPath: "/mnt/orphan",
	}})
	require.NoError(t, err)
	assert.False(t, abortCalled)
	assert.False(t, killCalled)
}

func TestReconcileStaleMountsEmitsEventBeforeDelete(t *testing.T) {
	provisioner := "rclone.csi.veloxpack.io"
	mountPath := filepath.Join(t.TempDir(), "pods", "live-uid", "volumes", "kubernetes.io~csi", "data", "mount")
	require.NoError(t, os.MkdirAll(filepath.Dir(mountPath), 0o755))
	require.NoError(t, writeVolData(filepath.Dir(mountPath), provisioner))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "default", UID: "live-uid"},
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
	recorder := record.NewFakeRecorder(4)
	r := NewReconciler(client, "node-1", provisioner)
	r.SetEventRecorder(recorder)
	r.confirmCorrupted = func(string) (bool, string) {
		return true, "mount point corrupted: input/output error"
	}

	err := r.ReconcileStaleMounts(context.Background(), []StaleMount{{
		PodUID: "live-uid", VolumeName: "data", MountPath: mountPath, Reason: "io error",
	}})
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("default").Get(context.Background(), "writer", metav1.GetOptions{})
	assert.Error(t, err)

	select {
	case evt := <-recorder.Events:
		assert.Contains(t, evt, corev1.EventTypeWarning)
		assert.Contains(t, evt, EventReasonStaleCSIMount)
		assert.Contains(t, evt, mountPath)
	case <-time.After(time.Second):
		t.Fatal("expected StaleCSIMount event")
	}
}

func TestReconcileWorkloadPodsAfterCSIRestartUsesLocalVolData(t *testing.T) {
	provisioner := "rclone.csi.veloxpack.io"
	kubeletDir := t.TempDir()
	podUID := "aaaa-bbbb"
	volDir := filepath.Join(kubeletDir, "pods", podUID, "volumes", csiVolumeDirSegment, "data")
	require.NoError(t, os.MkdirAll(volDir, 0o755))
	require.NoError(t, writeVolData(volDir, provisioner))

	ours := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "default", UID: types.UID(podUID)},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	foreign := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "media", Namespace: "media", UID: "cccc-dddd"},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "media-data"},
				},
			}},
		},
	}
	client := fake.NewSimpleClientset(ours, foreign)
	recorder := record.NewFakeRecorder(4)
	r := NewReconciler(client, "node-1", provisioner)
	r.SetKubeletDir(kubeletDir)
	r.SetEventRecorder(recorder)

	err := r.ReconcileWorkloadPodsAfterCSIRestart(context.Background())
	require.NoError(t, err)

	_, err = client.CoreV1().Pods("default").Get(context.Background(), "writer", metav1.GetOptions{})
	assert.Error(t, err)
	_, err = client.CoreV1().Pods("media").Get(context.Background(), "media", metav1.GetOptions{})
	require.NoError(t, err)

	select {
	case evt := <-recorder.Events:
		assert.Contains(t, evt, EventReasonCSINodeUIDChanged)
	case <-time.After(time.Second):
		t.Fatal("expected CSINodeUIDChanged event")
	}
}

func TestRestartPodTreatsNotFoundAsDone(t *testing.T) {
	client := fake.NewSimpleClientset() // pod absent
	r := NewReconciler(client, "node-1", "rclone.csi.veloxpack.io")
	recorder := record.NewFakeRecorder(2)
	r.SetEventRecorder(recorder)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gone",
			Namespace: "default",
			UID:       "uid-gone",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "gone-rs",
				UID:        "rs-uid",
				Controller: boolPtr(true),
			}},
		},
	}
	err := r.restartPod(context.Background(), pod, time.Now(), EventReasonCSINodeUIDChanged, "csi restarted")
	require.NoError(t, err)

	select {
	case evt := <-recorder.Events:
		assert.Contains(t, evt, EventReasonCSINodeUIDChanged)
	case <-time.After(time.Second):
		t.Fatal("expected owner event")
	}
	select {
	case evt := <-recorder.Events:
		t.Fatalf("unexpected second event (pod should not be annotated/evented when owner exists): %s", evt)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRestartPodAnnotatesOwnerAndReplacement(t *testing.T) {
	provisioner := "rclone.csi.veloxpack.io"
	ctrl := true
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "default", UID: "rs-uid"},
	}
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "writer-old",
			Namespace: "default",
			UID:       "old-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "writer",
				UID:        "rs-uid",
				Controller: &ctrl,
			}},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
	newPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "writer-new",
			Namespace: "default",
			UID:       "new-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "writer",
				UID:        "rs-uid",
				Controller: &ctrl,
			}},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
	client := fake.NewSimpleClientset(rs, oldPod)
	r := NewReconciler(client, "node-1", provisioner)
	r.replacementWait = 0

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	require.NoError(t, r.restartPod(context.Background(), oldPod, now, EventReasonStaleCSIMount, "stale"))

	_, err := client.CoreV1().Pods("default").Get(context.Background(), "writer-old", metav1.GetOptions{})
	assert.Error(t, err)

	gotRS, err := client.AppsV1().ReplicaSets("default").Get(context.Background(), "writer", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, now.Format(time.RFC3339), gotRS.Annotations[RecoveryAnnotation])

	_, err = client.CoreV1().Pods("default").Create(context.Background(), newPod, metav1.CreateOptions{})
	require.NoError(t, err)
	r.annotateReplacementPod(context.Background(), oldPod, now)

	gotNew, err := client.CoreV1().Pods("default").Get(context.Background(), "writer-new", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, now.Format(time.RFC3339), gotNew.Annotations[RecoveryAnnotation])
}

func boolPtr(v bool) *bool { return &v }
