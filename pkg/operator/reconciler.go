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
	"strings"
	"sync"
	"time"

	"github.com/veloxpack/csi-driver-rclone/pkg/rclone"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

const (
	// EventReasonStaleCSIMount is emitted before force-deleting a pod for a stale mount.
	EventReasonStaleCSIMount = "StaleCSIMount"
	// EventReasonCSINodeUIDChanged is emitted before force-deleting a pod after CSI node restart.
	EventReasonCSINodeUIDChanged = "CSINodeUIDChanged"

	defaultRecoveryCooldown = time.Hour
)

// Reconciler restarts workload pods with stale rclone CSI bind mounts.
type Reconciler struct {
	client                 kubernetes.Interface
	recorder               record.EventRecorder
	nodeName               string
	provisioner            string
	kubeletDir             string
	cooldown               time.Duration
	confirmCorrupted       func(path string) (bool, string)
	orphanLazyUmount       bool
	orphanFuseAbort        bool
	orphanKillMountProcess bool
	lazyUmount             func(path string) error             // overridable in tests
	resolveFuseConnID      func(path string) (string, error)   // overridable in tests
	fuseConnShared         func(path, id string) (bool, error) // overridable in tests
	abortFuseConn          func(id string) error               // overridable in tests
	killMountProcess       func(path string) error             // overridable in tests
	mu                     sync.Mutex                          // serializes pod deletes + lastRecovery
	lastRecovery           map[string]time.Time                // RecoveryKey → last restart
	replacementWait        time.Duration                       // best-effort wait to annotate recreated Pod
}

// NewReconciler builds a node-local pod reconciler.
func NewReconciler(client kubernetes.Interface, nodeName, provisioner string) *Reconciler {
	return &Reconciler{
		client:          client,
		nodeName:        nodeName,
		provisioner:     provisioner,
		cooldown:        defaultRecoveryCooldown,
		lazyUmount:      LazyUmount,
		lastRecovery:    map[string]time.Time{},
		replacementWait: defaultReplacementAnnotateWait,
	}
}

// SetEventRecorder sets the Kubernetes Event recorder used before pod restarts.
func (r *Reconciler) SetEventRecorder(recorder record.EventRecorder) {
	r.recorder = recorder
}

// SetKubeletDir sets the kubelet root used for local vol_data.json ownership checks.
func (r *Reconciler) SetKubeletDir(dir string) {
	r.kubeletDir = dir
}

// SetOrphanLazyUmount enables lazy-umount of stale CSI publish paths whose pod UID is gone.
func (r *Reconciler) SetOrphanLazyUmount(enabled bool) {
	r.orphanLazyUmount = enabled
}

// SetOrphanFuseAbort enables abort of the FUSE connection after orphan lazy-umount.
func (r *Reconciler) SetOrphanFuseAbort(enabled bool) {
	r.orphanFuseAbort = enabled
}

// SetOrphanKillMountProcess enables best-effort kill of hung mount servers after orphan umount.
func (r *Reconciler) SetOrphanKillMountProcess(enabled bool) {
	r.orphanKillMountProcess = enabled
}

// SetMountProbeTimeout sets the confirm probe used before pod restart.
// A positive timeout uses IsMountPathCorruptedWithTimeout; non-positive restores default.
func (r *Reconciler) SetMountProbeTimeout(timeout time.Duration) {
	if timeout <= 0 {
		r.confirmCorrupted = nil
		return
	}
	r.confirmCorrupted = func(path string) (bool, string) {
		return rclone.IsMountPathCorruptedWithTimeout(path, timeout)
	}
}

func (r *Reconciler) isMountCorrupted(path string) (bool, string) {
	if r.confirmCorrupted != nil {
		return r.confirmCorrupted(path)
	}
	return rclone.IsMountPathCorrupted(path)
}

// ReconcileStaleMounts deletes pods that have stale mounts, subject to skip and rate-limit rules.
func (r *Reconciler) ReconcileStaleMounts(ctx context.Context, stale []StaleMount) error {
	if len(stale) == 0 {
		return nil
	}

	pods, err := r.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + r.nodeName,
	})
	if err != nil {
		return fmt.Errorf("list pods on node %s: %w", r.nodeName, err)
	}

	byUID := make(map[string]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		byUID[string(pod.UID)] = pod
	}

	now := time.Now()
	for _, mount := range stale {
		pod, ok := byUID[mount.PodUID]
		if !ok {
			r.maybeLazyUmountOrphan(mount)
			continue
		}

		if ShouldSkipPod(pod) {
			klog.V(3).InfoS("skipping pod for stale mount recovery", "pod", pod.Namespace+"/"+pod.Name)
			continue
		}

		if !r.volumeManagedByProvisioner(ctx, pod, mount) {
			klog.V(4).InfoS("skipping pod volume not managed by provisioner",
				"pod", pod.Namespace+"/"+pod.Name, "volume", mount.VolumeName, "provisioner", r.provisioner)
			continue
		}

		if r.isRateLimited(ctx, pod, now) {
			klog.V(2).InfoS("rate limited stale mount recovery", "pod", pod.Namespace+"/"+pod.Name)
			continue
		}

		corrupted, reason := r.isMountCorrupted(mount.MountPath)
		if !corrupted {
			klog.V(3).InfoS(
				"skipping pod restart, mount recovered before reconcile",
				"pod", pod.Namespace+"/"+pod.Name, "path", mount.MountPath,
			)
			continue
		}
		klog.V(3).InfoS(
			"confirmed stale CSI mount before restart",
			"pod", pod.Namespace+"/"+pod.Name, "path", mount.MountPath, "reason", reason,
		)

		msg := fmt.Sprintf("stale CSI mount %s: %s", mount.MountPath, reason)
		if err := r.restartPod(ctx, pod, now, EventReasonStaleCSIMount, msg); err != nil {
			klog.ErrorS(err, "failed to restart pod for stale mount", "pod", pod.Namespace+"/"+pod.Name, "path", mount.MountPath)
			continue
		}

		klog.InfoS("restarted pod for stale CSI mount",
			"pod", pod.Namespace+"/"+pod.Name, "path", mount.MountPath, "reason", reason)
	}

	return nil
}

func (r *Reconciler) maybeLazyUmountOrphan(mount StaleMount) {
	klog.V(4).InfoS("stale mount pod not found on node", "podUID", mount.PodUID, "path", mount.MountPath)
	if !ShouldLazyUmountOrphan(r.orphanLazyUmount, false) {
		return
	}

	// Capture FUSE conn id before umount removes the mountinfo row.
	var connID string
	resolve := r.resolveFuseConnID
	if resolve == nil {
		resolve = ResolveFuseConnID
	}
	if id, err := resolve(mount.MountPath); err != nil {
		klog.V(3).InfoS("orphan fuse conn id lookup failed", "path", mount.MountPath, "err", err)
	} else {
		connID = id
	}

	umount := r.lazyUmount
	if umount == nil {
		umount = LazyUmount
	}
	if err := umount(mount.MountPath); err != nil {
		klog.ErrorS(err, "orphan lazy umount failed", "podUID", mount.PodUID, "path", mount.MountPath)
		return
	}
	klog.InfoS("lazy-umounted orphan CSI mount", "podUID", mount.PodUID, "path", mount.MountPath)

	if ShouldAbortOrphanFuse(r.orphanFuseAbort, false) && connID != "" {
		isShared := r.fuseConnShared
		if isShared == nil {
			isShared = fuseConnSharedByOtherMount
		}
		shared, err := isShared(mount.MountPath, connID)
		if err != nil {
			klog.V(3).InfoS("orphan fuse connection sharing check failed",
				"connID", connID, "path", mount.MountPath, "err", err)
		} else if shared {
			klog.V(2).InfoS("skipping abort of shared orphan fuse connection",
				"connID", connID, "path", mount.MountPath)
		} else {
			abort := r.abortFuseConn
			if abort == nil {
				abort = AbortFuseConn
			}
			if err := abort(connID); err != nil {
				klog.ErrorS(err, "orphan fuse abort failed", "connID", connID, "path", mount.MountPath)
			} else {
				klog.InfoS("aborted orphan fuse connection", "connID", connID, "path", mount.MountPath)
			}
		}
	}

	if ShouldKillOrphanMountProcess(r.orphanKillMountProcess, false) {
		kill := r.killMountProcess
		if kill == nil {
			kill = BestEffortKillMountServer
		}
		if err := kill(mount.MountPath); err != nil {
			klog.ErrorS(err, "orphan mount process kill failed", "path", mount.MountPath)
		}
	}
}

// ReconcileWorkloadPodsAfterCSIRestart restarts workload pods using rclone volumes after a CSI node restart.
// Kubelet publish paths may already be healthy (Phase B remount) while container bind mounts stay stale.
func (r *Reconciler) ReconcileWorkloadPodsAfterCSIRestart(ctx context.Context) error {
	pods, err := r.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + r.nodeName,
	})
	if err != nil {
		return fmt.Errorf("list pods on node %s: %w", r.nodeName, err)
	}

	now := time.Now()
	for i := range pods.Items {
		pod := &pods.Items[i]
		if ShouldSkipPod(pod) {
			continue
		}
		if !r.podUsesProvisioner(ctx, pod) {
			continue
		}
		if r.isRateLimited(ctx, pod, now) {
			klog.V(2).InfoS("rate limited CSI restart recovery", "pod", pod.Namespace+"/"+pod.Name)
			continue
		}
		msg := fmt.Sprintf("CSI node pod restarted on node %s; refreshing volume binds", r.nodeName)
		if err := r.restartPod(ctx, pod, now, EventReasonCSINodeUIDChanged, msg); err != nil {
			klog.ErrorS(err, "failed to restart pod after CSI node restart", "pod", pod.Namespace+"/"+pod.Name)
			continue
		}
		klog.InfoS("restarted pod after CSI node restart", "pod", pod.Namespace+"/"+pod.Name)
	}
	return nil
}

func (r *Reconciler) volumeManagedByProvisioner(ctx context.Context, pod *corev1.Pod, mount StaleMount) bool {
	if managed, known := CSIMountManagedBy(mount.MountPath, r.provisioner); known {
		return managed
	}
	return podUsesProvisionerVolume(ctx, r.client, pod, mount.VolumeName, r.provisioner)
}

func (r *Reconciler) podUsesProvisioner(ctx context.Context, pod *corev1.Pod) bool {
	if r.kubeletDir != "" {
		if has, known := PodHasLocalProvisionerVolume(r.kubeletDir, string(pod.UID), r.provisioner); known {
			return has
		}
	}
	return podHasProvisionerVolume(ctx, r.client, pod, r.provisioner)
}

func podHasProvisionerVolume(
	ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, provisioner string,
) bool {
	for _, vol := range pod.Spec.Volumes {
		if vol.CSI != nil && vol.CSI.Driver == provisioner {
			return true
		}
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		pvc, err := client.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(
			ctx, vol.PersistentVolumeClaim.ClaimName, metav1.GetOptions{},
		)
		if err != nil {
			klog.V(4).InfoS("failed to get PVC for volume", "pvc", vol.PersistentVolumeClaim.ClaimName, "err", err)
			continue
		}
		if pvc.Spec.VolumeName == "" {
			continue
		}
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			klog.V(4).InfoS("failed to get PV for PVC", "pv", pvc.Spec.VolumeName, "err", err)
			continue
		}
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == provisioner {
			return true
		}
	}
	return false
}

func (r *Reconciler) restartPod(ctx context.Context, pod *corev1.Pod, now time.Time, reason, message string) error {
	// Emit before delete so the Event remains associated with the controller owner.
	r.emitRecoveryEvents(pod, reason, message)
	knownPodUIDs, err := r.snapshotOwnerPodUIDs(ctx, pod)
	if err != nil {
		klog.V(2).InfoS("failed to snapshot controller pods for replacement annotation",
			"pod", pod.Namespace+"/"+pod.Name, "err", err)
		knownPodUIDs = nil
	}

	grace := int64(0)
	deletePolicy := metav1.DeletePropagationBackground
	err = r.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
		PropagationPolicy:  &deletePolicy,
	})
	if apierrors.IsNotFound(err) {
		klog.V(2).InfoS("pod already gone before delete; treating restart as done",
			"pod", pod.Namespace+"/"+pod.Name, "reason", reason)
		err = nil
	}
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.markRecoveredLocked(pod, now)
	r.mu.Unlock()
	if err := r.annotateOwner(ctx, pod, now); err != nil {
		klog.V(2).InfoS("failed to annotate recovery owner",
			"pod", pod.Namespace+"/"+pod.Name, "err", err)
	}

	// Best-effort: annotate the replacement Pod once the controller recreates it.
	r.scheduleReplacementAnnotate(pod, now, knownPodUIDs)
	return nil
}

func (r *Reconciler) emitRecoveryEvents(pod *corev1.Pod, reason, message string) {
	if r.recorder == nil {
		return
	}
	// Prefer controller owner so the Event survives Pod recreate.
	if owner := controllerOwnerObject(pod); owner != nil {
		r.recorder.Event(owner, corev1.EventTypeWarning, reason, message)
		return
	}
	r.recorder.Event(pod, corev1.EventTypeWarning, reason, message)
}

// controllerOwnerObject returns a minimal runtime.Object for the pod's controller owner
// so Events remain visible on ReplicaSet/StatefulSet/Job after the Pod UID churns.
func controllerOwnerObject(pod *corev1.Pod) runtime.Object {
	for i := range pod.OwnerReferences {
		o := &pod.OwnerReferences[i]
		if o.Controller == nil || !*o.Controller {
			continue
		}
		return &corev1.ObjectReference{
			APIVersion: o.APIVersion,
			Kind:       o.Kind,
			Namespace:  pod.Namespace,
			Name:       o.Name,
			UID:        o.UID,
		}
	}
	return nil
}

// ShouldSkipPod reports whether a pod must not be restarted by the recovery operator.
func ShouldSkipPod(pod *corev1.Pod) bool {
	if pod.Namespace == "kube-system" {
		return true
	}
	if strings.Contains(pod.Name, "csi-rclone") {
		return true
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind != "DaemonSet" {
			continue
		}
		name := strings.ToLower(owner.Name)
		if strings.Contains(name, "csi-rclone") || strings.Contains(name, "csi-rclone-node") {
			return true
		}
	}
	return false
}

// RecoveryKey identifies a workload across Pod recreates (controller owner, else Pod name).
func RecoveryKey(pod *corev1.Pod) string {
	for _, o := range pod.OwnerReferences {
		if o.Controller == nil || !*o.Controller {
			continue
		}
		return pod.Namespace + "/" + o.Kind + "/" + o.Name
	}
	return pod.Namespace + "/Pod/" + pod.Name
}

func (r *Reconciler) isRateLimited(ctx context.Context, pod *corev1.Pod, now time.Time) bool {
	r.mu.Lock()
	r.pruneExpiredRecoveriesLocked(now)
	last, ok := r.lastRecovery[RecoveryKey(pod)]
	cooldown := r.cooldown
	r.mu.Unlock()
	if ok && now.Sub(last) < cooldown {
		return true
	}
	return r.ownerAnnotationRateLimited(ctx, pod, now)
}

func (r *Reconciler) markRecoveredLocked(pod *corev1.Pod, now time.Time) {
	if r.lastRecovery == nil {
		r.lastRecovery = map[string]time.Time{}
	}
	r.pruneExpiredRecoveriesLocked(now)
	r.lastRecovery[RecoveryKey(pod)] = now
}

func (r *Reconciler) pruneExpiredRecoveriesLocked(now time.Time) {
	cutoff := now.Add(-r.cooldown)
	for key, last := range r.lastRecovery {
		if !last.After(cutoff) {
			delete(r.lastRecovery, key)
		}
	}
}

func podUsesProvisionerVolume(
	ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, volumeName, provisioner string,
) bool {
	for _, vol := range pod.Spec.Volumes {
		if vol.Name != volumeName {
			continue
		}
		if vol.CSI != nil {
			return vol.CSI.Driver == provisioner
		}
		if vol.PersistentVolumeClaim == nil {
			return false
		}
		pvc, err := client.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(
			ctx, vol.PersistentVolumeClaim.ClaimName, metav1.GetOptions{},
		)
		if err != nil {
			klog.V(4).InfoS("failed to get PVC for volume", "pvc", vol.PersistentVolumeClaim.ClaimName, "err", err)
			return false
		}
		if pvc.Spec.VolumeName == "" {
			return false
		}
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			klog.V(4).InfoS("failed to get PV for PVC", "pv", pvc.Spec.VolumeName, "err", err)
			return false
		}
		if pv.Spec.CSI == nil {
			return false
		}
		return pv.Spec.CSI.Driver == provisioner
	}
	return false
}
