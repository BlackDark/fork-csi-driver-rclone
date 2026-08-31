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
	"sync"
	"time"
)

// RecoveryLoops runs CSI-UID recovery and stale-mount scan on independent tickers
// so a hung FUSE probe cannot starve CSI restart detection.
type RecoveryLoops struct {
	Interval time.Duration
	OnCSI    func(context.Context) // optional; nil skips CSI loop
	OnScan   func(context.Context)
}

// Run starts the configured loops and blocks until ctx is cancelled, then waits for
// in-flight tick handlers to return (handlers must respect ctx).
func (r *RecoveryLoops) Run(ctx context.Context) {
	var wg sync.WaitGroup
	start := func(fn func(context.Context)) {
		if fn == nil {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(r.Interval)
			defer ticker.Stop()
			fn(ctx)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					fn(ctx)
				}
			}
		}()
	}

	start(r.OnCSI)
	start(r.OnScan)
	<-ctx.Done()
	wg.Wait()
}
