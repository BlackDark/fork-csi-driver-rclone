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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoveryLoopsCSINotBlockedByHungScan(t *testing.T) {
	var csiCalls atomic.Int32
	scanStarted := make(chan struct{})
	scanRelease := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		loops := RecoveryLoops{
			Interval: 20 * time.Millisecond,
			OnCSI: func(context.Context) {
				csiCalls.Add(1)
			},
			OnScan: func(ctx context.Context) {
				select {
				case <-scanStarted:
				default:
					close(scanStarted)
				}
				select {
				case <-scanRelease:
				case <-ctx.Done():
				}
			},
		}
		loops.Run(ctx)
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan loop did not start")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if csiCalls.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.GreaterOrEqual(t, csiCalls.Load(), int32(2), "CSI loop must keep ticking while scan is hung")

	cancel()
	close(scanRelease)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RecoveryLoops.Run did not exit")
	}
}

func TestShouldLazyUmountOrphan(t *testing.T) {
	assert.True(t, ShouldLazyUmountOrphan(true, false))
	assert.False(t, ShouldLazyUmountOrphan(true, true))
	assert.False(t, ShouldLazyUmountOrphan(false, false))
	assert.False(t, ShouldLazyUmountOrphan(false, true))
}
