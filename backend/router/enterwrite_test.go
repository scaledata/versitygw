// Copyright 2024 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package router

import (
	"context"
	"testing"
	"time"
)

// TestEnterWriteLocalRespectsContextCancel verifies the local byte-write path
// no longer blocks forever on a busy (or wedged) channel: when the per-channel
// semaphore is held and the caller's context is already done, enterWrite fails
// fast with the context error instead of parking a goroutine on the send.
func TestEnterWriteLocalRespectsContextCancel(t *testing.T) {
	r := &Router{chanSem: make(chan struct{}, 1)}
	r.chanSem <- struct{}{} // occupy it so a second local acquire must wait

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, release, err := r.enterWrite(ctx, true)
		release()
		if err == nil {
			t.Error("enterWrite returned nil error on a cancelled ctx while the semaphore was held")
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enterWrite blocked on a full semaphore despite a cancelled context")
	}
}

// TestEnterWriteLocalAcquiresAndReleases verifies the happy path still takes and
// frees the per-channel semaphore.
func TestEnterWriteLocalAcquiresAndReleases(t *testing.T) {
	r := &Router{chanSem: make(chan struct{}, 1)}

	_, release, err := r.enterWrite(context.Background(), true)
	if err != nil {
		t.Fatalf("enterWrite: %v", err)
	}
	// The semaphore must be held now.
	select {
	case r.chanSem <- struct{}{}:
		t.Fatal("semaphore was not held after a local enterWrite")
	default:
	}

	release()
	// ...and free after release.
	select {
	case r.chanSem <- struct{}{}:
		<-r.chanSem
	default:
		t.Fatal("semaphore was not released")
	}
}
