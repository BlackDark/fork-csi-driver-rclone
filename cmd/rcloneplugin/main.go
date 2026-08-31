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

	metricsserver "github.com/veloxpack/csi-driver-rclone/internal/metrics"
	rcserver "github.com/veloxpack/csi-driver-rclone/internal/rc"
	"github.com/veloxpack/csi-driver-rclone/pkg/rclone"
	"k8s.io/klog/v2"
)

var (
	endpoint     = flag.String("endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	nodeID       = flag.String("nodeid", "", "node id")
	driverName   = flag.String("drivername", rclone.DefaultDriverName, "name of the driver")
	remount      = flag.Bool("remount", false, "remount existing volume mount points on startup")
	staging      = flag.Bool("staging", false, "enable CSI NodeStageVolume staging path")
	mountTimeout = flag.Duration("mount-timeout", 90*time.Second,
		"maximum time NodePublishVolume waits for a mount before returning and reaping it in the background")
)

func main() {
	klog.InitFlags(nil)
	_ = flag.Set("logtostderr", "true")

	metricsOpts := metricsserver.NewOptions()
	rcOpts := rcserver.NewOptions()

	flag.StringVar(&metricsOpts.MetricsAddr, "metrics-addr",
		metricsOpts.MetricsAddr, "Metrics server listening address")
	flag.StringVar(&metricsOpts.MetricsPath, "metrics-path",
		metricsOpts.MetricsPath, "HTTP path where metrics are exposed")
	flag.DurationVar(&metricsOpts.ReadTimeout, "metrics-server-read-timeout",
		metricsOpts.ReadTimeout, "Metrics server read timeout")
	flag.DurationVar(&metricsOpts.WriteTimeout, "metrics-server-write-timeout",
		metricsOpts.WriteTimeout, "Metrics server write timeout")
	flag.DurationVar(&metricsOpts.IdleTimeout, "metrics-server-idle-timeout",
		metricsOpts.IdleTimeout, "Metrics server idle timeout")
	flag.BoolVar(&rcOpts.Enabled, "rc",
		rcOpts.Enabled, "Enable rclone Remote Control (RC) API")
	flag.StringVar(&rcOpts.Address, "rc-addr",
		rcOpts.Address, "RC server listening address")
	flag.BoolVar(&rcOpts.NoAuth, "rc-no-auth",
		rcOpts.NoAuth, "Disable authentication for RC (insecure)")
	flag.StringVar(&rcOpts.Username, "rc-user",
		rcOpts.Username, "RC basic auth username")
	flag.StringVar(&rcOpts.Password, "rc-pass",
		rcOpts.Password, "RC basic auth password")

	flag.Parse()

	if *nodeID == "" {
		klog.Warning("nodeid is empty")
	}

	ctx := context.Background()

	var metricsSrv interface {
		Addr() string
		Shutdown(context.Context) error
	}
	var rcSrv rcserver.Server

	if metricsOpts.MetricsAddr != "" {
		srv, err := metricsserver.Start(metricsOpts)
		if err != nil {
			klog.Fatalf("Failed to start metrics server: %v", err)
		}
		if srv != nil {
			metricsSrv = srv
			klog.Infof("Metrics server listening on http://%s%s", srv.Addr(), metricsOpts.MetricsPath)
		}
	}

	if rcOpts.Enabled {
		srv, err := rcserver.Start(ctx, rcOpts)
		if err != nil {
			klog.Fatalf("Failed to start RC server: %v", err)
		}
		if srv != nil {
			rcSrv = srv
			klog.Infof("RC server listening on %s", rcOpts.Address)
		}
	}

	driverOptions := rclone.DriverOptions{
		NodeID:       *nodeID,
		DriverName:   *driverName,
		Endpoint:     *endpoint,
		Remount:      *remount,
		Staging:      *staging,
		MountTimeout: *mountTimeout,
	}

	driver := rclone.NewDriver(&driverOptions)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGUSR1,
		syscall.SIGUSR2,
	)

	go driver.Run(false)

	for sig := range sigChan {
		klog.Infof("Received signal: %v", sig)
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			shutdownCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			if metricsSrv != nil {
				metricsCtx, metricsCancel := context.WithTimeout(ctx, 5*time.Second)
				if err := metricsSrv.Shutdown(metricsCtx); err != nil {
					klog.Errorf("Error shutting down metrics server: %v", err)
				}
				metricsCancel()
			}
			if rcSrv != nil {
				if err := rcSrv.Shutdown(); err != nil {
					klog.Errorf("Error shutting down RC server: %v", err)
				}
			}
			err := driver.Shutdown(shutdownCtx)
			cancel()
			if err != nil {
				klog.Errorf("Error during driver shutdown: %v", err)
				return
			}
			klog.Info("Graceful shutdown completed")
			return
		case syscall.SIGUSR1:
			driver.DumpMountInfo()
		case syscall.SIGUSR2:
			if err := driver.ForceCacheSync(ctx); err != nil {
				klog.Errorf("Cache sync failed: %v", err)
			}
		}
	}
}
