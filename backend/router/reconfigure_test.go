package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"
)

// reconfigureTestBackend is a no-op Backend for concurrency testing.
type reconfigureTestBackend struct {
	backend.BackendUnsupported
	id int
}

func (b *reconfigureTestBackend) String() string { return "test" }

// TestReconfigureRaceClean verifies that Reconfigure racing with concurrent
// PutObject/GetObject/pick calls does not cause a data race.
func TestReconfigureRaceClean(t *testing.T) {
	be1 := &reconfigureTestBackend{id: 1}
	be2 := &reconfigureTestBackend{id: 2}

	om := &OwnerMap{N: 1, Epoch: 0, Slots: []Slot{{Slot: 0}}}
	r, err := New(be1, []backend.Backend{nil}, om, 0, 0)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Goroutine: continuously pick and read the local backend
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = r.pick("bucket", "key")
				_ = r.cfg.Load().local
			}
		}
	}()

	// Goroutine: continuously call delegate methods
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = r.ListBuckets(context.Background(), s3response.ListBucketsInput{})
			}
		}
	}()

	// Main: Reconfigure rapidly
	for i := 0; i < 50; i++ {
		if err := r.Reconfigure(context.Background(), be2, []backend.Backend{nil}, 0, 1); err != nil {
			t.Errorf("reconfigure be2: %v", err)
		}
		if err := r.Reconfigure(context.Background(), be1, []backend.Backend{nil}, 0, 1); err != nil {
			t.Errorf("reconfigure be1: %v", err)
		}
		time.Sleep(time.Microsecond)
	}

	close(stop)
	wg.Wait()
}

// shutdownRecorder records whether Shutdown was called.
type shutdownRecorder struct {
	backend.BackendUnsupported
	shut bool
}

func (s *shutdownRecorder) Shutdown() { s.shut = true }

// TestReconfigureShutsDownReplaced verifies the replaced local backend is shut
// down (releasing e.g. posix's root fd) while a reused instance is not.
func TestReconfigureShutsDownReplaced(t *testing.T) {
	oldBE := &shutdownRecorder{}
	newBE := &shutdownRecorder{}
	om := &OwnerMap{N: 1, Epoch: 0, Slots: []Slot{{Slot: 0}}}
	r, err := New(oldBE, []backend.Backend{nil}, om, 0, 0)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	if err := r.Reconfigure(context.Background(), newBE, []backend.Backend{nil}, 0, 1); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if !oldBE.shut {
		t.Error("replaced backend was not Shutdown (fd leak)")
	}
	if newBE.shut {
		t.Error("newly installed backend must not be Shutdown")
	}
}

// TestReconfigureValidation verifies a mismatched configuration is rejected and
// leaves the existing config untouched (rather than installing a snapshot that
// panics pick()); and that selfIdx == -1 (pure forwarder) is accepted.
func TestReconfigureValidation(t *testing.T) {
	be := &reconfigureTestBackend{id: 1}
	peer := &reconfigureTestBackend{id: 2}
	om := &OwnerMap{N: 1, Epoch: 0, Slots: []Slot{{Slot: 0}}}
	r, err := New(be, []backend.Backend{nil}, om, 0, 0)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	// newN=5 but only one peer slot -> reject.
	if err := r.Reconfigure(context.Background(), be, []backend.Backend{nil}, 0, 5); err == nil {
		t.Fatal("Reconfigure accepted mismatched peers/n")
	}
	if c := r.cfg.Load(); c.n != 1 || c.local != backend.Backend(be) {
		t.Fatalf("rejected Reconfigure mutated the live config: n=%d", c.n)
	}

	// selfIdx == -1 (pure forwarder) with all peers non-nil -> accept.
	if err := r.Reconfigure(context.Background(), be, []backend.Backend{peer}, -1, 1); err != nil {
		t.Fatalf("pure-forwarder Reconfigure rejected: %v", err)
	}
}
