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

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/veloxpack/csi-driver-rclone/pkg/operator"
	"github.com/veloxpack/csi-driver-rclone/pkg/rclone"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

var (
	kubeletDir   = flag.String("kubelet-dir", "/var/lib/kubelet", "Kubelet state directory")
	provisioner  = flag.String("provisioner", rclone.DefaultDriverName, "CSI driver name to recover")
	scanInterval = flag.Duration("scan-interval", 60*time.Second, "Interval between stale mount scans")
	nodeName     = flag.String("node-name", "", "Kubernetes node name (defaults to NODE_NAME env)")
	csiNodeLabel = flag.String(
		"csi-node-label", operator.DefaultCSINodeLabelSelector(),
		"Label selector for CSI node pods",
	)
	csiRestartRecovery = flag.Bool(
		"csi-restart-recovery", true,
		"Restart workload pods when the CSI node pod restarts",
	)
	csiRestartReadyTimeout = flag.Duration(
		"csi-restart-ready-timeout", operator.DefaultCSIRestartReadyTimeout(),
		"Max wait for CSI node pod Ready after a restart before deleting workloads",
	)
	mountProbeTimeout = flag.Duration(
		"mount-probe-timeout", 3*time.Second,
		"Max wait for mount health probe; timeout is treated as corrupted",
	)
	orphanLazyUmount = flag.Bool(
		"orphan-lazy-umount", true,
		"Lazy-umount stale CSI publish paths when the pod UID is gone from the node",
	)
)

func main() {
	klog.InitFlags(nil)
	_ = flag.Set("logtostderr", "true")
	flag.Parse()

	if *nodeName == "" {
		*nodeName = os.Getenv("NODE_NAME")
	}
	if *nodeName == "" {
		klog.Fatal("node name is required: set --node-name or NODE_NAME")
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("failed to load in-cluster config: %v", err)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to create kubernetes client: %v", err)
	}

	mounter := mount.New("" /* mounterPath */)
	reconciler := operator.NewReconciler(client, *nodeName, *provisioner)
	reconciler.SetOrphanLazyUmount(*orphanLazyUmount)
	reconciler.SetMountProbeTimeout(*mountProbeTimeout)
	operator.ConfigureMountProbe(*mountProbeTimeout)
	csiTracker := operator.NewCSINodeTracker(client, *nodeName, *csiNodeLabel)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	klog.InfoS("starting volume recovery operator",
		"node", *nodeName,
		"kubeletDir", *kubeletDir,
		"provisioner", *provisioner,
		"scanInterval", *scanInterval,
		"csiNodeLabel", *csiNodeLabel,
		"csiRestartRecovery", *csiRestartRecovery,
		"csiRestartReadyTimeout", *csiRestartReadyTimeout,
		"mountProbeTimeout", *mountProbeTimeout,
		"orphanLazyUmount", *orphanLazyUmount)

	var onCSI func(context.Context)
	if *csiRestartRecovery {
		onCSI = func(ctx context.Context) {
			restarted, err := csiTracker.CheckRestarted(ctx)
			if err != nil {
				klog.ErrorS(err, "CSI node restart check failed")
				return
			}
			if !restarted {
				return
			}
			if err := csiTracker.WaitUntilReady(ctx, *csiRestartReadyTimeout); err != nil {
				klog.ErrorS(err, "waiting for CSI node Ready failed; skipping CSI restart recovery")
				return
			}
			if err := reconciler.ReconcileWorkloadPodsAfterCSIRestart(ctx); err != nil {
				klog.ErrorS(err, "CSI restart workload recovery failed")
			}
		}
	}

	loops := operator.RecoveryLoops{
		Interval: *scanInterval,
		OnCSI:    onCSI,
		OnScan: func(ctx context.Context) {
			stale, err := operator.ScanStaleMounts(*kubeletDir, mounter)
			if err != nil {
				klog.ErrorS(err, "stale mount scan failed")
				return
			}
			if len(stale) > 0 {
				klog.InfoS("stale mounts detected", "count", len(stale))
			}
			if err := reconciler.ReconcileStaleMounts(ctx, stale); err != nil {
				klog.ErrorS(err, "stale mount reconciliation failed")
			}
		},
	}
	loops.Run(ctx)
	klog.Info("volume recovery operator shutting down")
}
