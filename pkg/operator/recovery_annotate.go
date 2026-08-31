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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// RecoveryAnnotation records the last recovery time on the controller owner
// and, best-effort, on the replacement Pod after recreate.
const RecoveryAnnotation = "volume.veloxpack.io/last-recovery"

const defaultReplacementAnnotateWait = 30 * time.Second

func controllerOwnerRef(pod *corev1.Pod) *metav1.OwnerReference {
	for i := range pod.OwnerReferences {
		o := &pod.OwnerReferences[i]
		if o.Controller != nil && *o.Controller {
			return o
		}
	}
	return nil
}

func setRecoveryAnnotation(meta *metav1.ObjectMeta, now time.Time) {
	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}
	meta.Annotations[RecoveryAnnotation] = now.UTC().Format(time.RFC3339)
}

func parseRecoveryAnnotation(meta metav1.ObjectMeta, now time.Time, cooldown time.Duration) bool {
	raw, ok := meta.Annotations[RecoveryAnnotation]
	if !ok || raw == "" {
		return false
	}
	last, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return true
	}
	return now.Sub(last) < cooldown
}

func (r *Reconciler) annotateOwner(ctx context.Context, pod *corev1.Pod, now time.Time) error {
	o := controllerOwnerRef(pod)
	if o == nil {
		return nil
	}
	ns := pod.Namespace
	switch o.Kind {
	case "ReplicaSet":
		rs, err := r.client.AppsV1().ReplicaSets(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		rs = rs.DeepCopy()
		setRecoveryAnnotation(&rs.ObjectMeta, now)
		_, err = r.client.AppsV1().ReplicaSets(ns).Update(ctx, rs, metav1.UpdateOptions{})
		return err
	case "StatefulSet":
		sts, err := r.client.AppsV1().StatefulSets(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		sts = sts.DeepCopy()
		setRecoveryAnnotation(&sts.ObjectMeta, now)
		_, err = r.client.AppsV1().StatefulSets(ns).Update(ctx, sts, metav1.UpdateOptions{})
		return err
	case "DaemonSet":
		ds, err := r.client.AppsV1().DaemonSets(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		ds = ds.DeepCopy()
		setRecoveryAnnotation(&ds.ObjectMeta, now)
		_, err = r.client.AppsV1().DaemonSets(ns).Update(ctx, ds, metav1.UpdateOptions{})
		return err
	case "Job":
		job, err := r.client.BatchV1().Jobs(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		job = job.DeepCopy()
		setRecoveryAnnotation(&job.ObjectMeta, now)
		_, err = r.client.BatchV1().Jobs(ns).Update(ctx, job, metav1.UpdateOptions{})
		return err
	default:
		klog.V(4).InfoS("skip owner annotate for unsupported kind",
			"pod", ns+"/"+pod.Name, "kind", o.Kind, "name", o.Name)
		return nil
	}
}

func (r *Reconciler) ownerAnnotationRateLimited(ctx context.Context, pod *corev1.Pod, now time.Time) bool {
	o := controllerOwnerRef(pod)
	if o == nil {
		return false
	}
	ns := pod.Namespace
	var meta metav1.ObjectMeta
	switch o.Kind {
	case "ReplicaSet":
		rs, err := r.client.AppsV1().ReplicaSets(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		meta = rs.ObjectMeta
	case "StatefulSet":
		sts, err := r.client.AppsV1().StatefulSets(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		meta = sts.ObjectMeta
	case "DaemonSet":
		ds, err := r.client.AppsV1().DaemonSets(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		meta = ds.ObjectMeta
	case "Job":
		job, err := r.client.BatchV1().Jobs(ns).Get(ctx, o.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		meta = job.ObjectMeta
	default:
		return false
	}
	return parseRecoveryAnnotation(meta, now, r.cooldown)
}

func ownedByController(pod *corev1.Pod, owner *metav1.OwnerReference) bool {
	for _, o := range pod.OwnerReferences {
		if o.UID == owner.UID && o.Kind == owner.Kind && o.Name == owner.Name {
			return true
		}
	}
	return false
}

func (r *Reconciler) snapshotOwnerPodUIDs(ctx context.Context, old *corev1.Pod) (map[string]struct{}, error) {
	owner := controllerOwnerRef(old)
	if owner == nil {
		return nil, nil
	}
	pods, err := r.client.CoreV1().Pods(old.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	known := make(map[string]struct{}, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if ownedByController(pod, owner) {
			known[string(pod.UID)] = struct{}{}
		}
	}
	return known, nil
}

// annotateReplacementPod waits for a new Pod with the same controller and annotates it.
func (r *Reconciler) annotateReplacementPod(
	ctx context.Context, old *corev1.Pod, now time.Time, knownPodUIDs map[string]struct{},
) {
	owner := controllerOwnerRef(old)
	if owner == nil {
		return
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := r.tryAnnotateReplacementOnce(ctx, old, owner, now, knownPodUIDs); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			klog.V(2).InfoS("timed out waiting to annotate replacement pod",
				"oldPod", old.Namespace+"/"+old.Name, "owner", owner.Kind+"/"+owner.Name)
			return
		case <-ticker.C:
		}
	}
}

func (r *Reconciler) tryAnnotateReplacementOnce(
	ctx context.Context, old *corev1.Pod, owner *metav1.OwnerReference, now time.Time,
	knownPodUIDs map[string]struct{},
) error {
	pods, err := r.client.CoreV1().Pods(old.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.UID == old.UID {
			continue
		}
		if !ownedByController(p, owner) {
			continue
		}
		if _, known := knownPodUIDs[string(p.UID)]; known {
			continue
		}
		p = p.DeepCopy()
		setRecoveryAnnotation(&p.ObjectMeta, now)
		if _, err := r.client.CoreV1().Pods(p.Namespace).Update(ctx, p, metav1.UpdateOptions{}); err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				return fmt.Errorf("retry: %w", err)
			}
			klog.V(2).InfoS("failed to annotate replacement pod",
				"pod", p.Namespace+"/"+p.Name, "err", err)
			return err
		}
		klog.V(2).InfoS("annotated replacement pod after recovery",
			"pod", p.Namespace+"/"+p.Name, "owner", owner.Kind+"/"+owner.Name)
		return nil
	}
	return fmt.Errorf("replacement pod not ready yet")
}

func (r *Reconciler) scheduleReplacementAnnotate(
	old *corev1.Pod, now time.Time, knownPodUIDs map[string]struct{},
) {
	wait := r.replacementWait
	if wait <= 0 {
		return
	}
	if knownPodUIDs == nil {
		return
	}
	old = old.DeepCopy()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		r.annotateReplacementPod(ctx, old, now, knownPodUIDs)
	}()
}
