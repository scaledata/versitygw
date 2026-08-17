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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
)

// fakeBucketBackend counts CreateBucket/DeleteBucket calls and returns
// configurable errors, so fan-out behavior can be asserted without real storage.
type fakeBucketBackend struct {
	backend.BackendUnsupported
	createErr error
	deleteErr error
	creates   int
	deletes   int
}

func (f *fakeBucketBackend) CreateBucket(_ context.Context, _ *s3.CreateBucketInput, _ []byte) error {
	f.creates++
	return f.createErr
}

func (f *fakeBucketBackend) DeleteBucket(_ context.Context, _ string) error {
	f.deletes++
	return f.deleteErr
}

func TestNewValidation(t *testing.T) {
	m := &OwnerMap{N: 3, Epoch: 1}
	local := &fakeBucketBackend{}
	goodPeers := func() []backend.Backend {
		return []backend.Backend{local, &fakeBucketBackend{}, &fakeBucketBackend{}}
	}

	cases := []struct {
		name    string
		local   backend.Backend
		peers   []backend.Backend
		selfIdx int
		wantErr bool
	}{
		{"nil local", nil, goodPeers(), 0, true},
		{"selfIdx -1 pure forwarder", local, goodPeers(), -1, false},
		{"selfIdx < -1", local, goodPeers(), -2, true},
		{"selfIdx == N", local, goodPeers(), 3, true},
		{"peers len mismatch", local, []backend.Backend{local, &fakeBucketBackend{}}, 0, true},
		{"nil non-self peer", local, []backend.Backend{local, nil, &fakeBucketBackend{}}, 0, true},
		{"valid", local, goodPeers(), 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.local, tc.peers, m, tc.selfIdx, 0, 1)
			if (err != nil) != tc.wantErr {
				t.Fatalf("New err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestCreateBucketFanOutAndEcho(t *testing.T) {
	m := &OwnerMap{N: 3, Epoch: 1}

	// Fresh local create fans out to both peers.
	local := &fakeBucketBackend{}
	p1, p2 := &fakeBucketBackend{}, &fakeBucketBackend{}
	r, err := New(local, []backend.Backend{local, p1, p2}, m, 0, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.CreateBucket(context.Background(), &s3.CreateBucketInput{}, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if p1.creates != 1 || p2.creates != 1 {
		t.Fatalf("expected fan-out to both peers, got p1=%d p2=%d", p1.creates, p2.creates)
	}

	// Echo: the local create reports AlreadyExists, so this is a fan-out arriving
	// at a node that already has the bucket — it must NOT re-fan.
	echoLocal := &fakeBucketBackend{createErr: s3err.GetAPIError(s3err.ErrBucketAlreadyExists)}
	q1, q2 := &fakeBucketBackend{}, &fakeBucketBackend{}
	er, err := New(echoLocal, []backend.Backend{echoLocal, q1, q2}, m, 0, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := er.CreateBucket(context.Background(), &s3.CreateBucketInput{}, nil); err != nil {
		t.Fatalf("echo CreateBucket should return nil, got %v", err)
	}
	if q1.creates != 0 || q2.creates != 0 {
		t.Fatalf("echo must not re-fan, got q1=%d q2=%d", q1.creates, q2.creates)
	}
}

func TestLoadOwnerMapSlotIndex(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "owner.json")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		return p
	}

	// slot fields disagree with array position -> rejected.
	misordered := `{"epoch":1,"n":2,"slots":[
		{"slot":1,"nodeId":"b","endpoint":"10.0.0.2:9000","channelPath":"/c1"},
		{"slot":0,"nodeId":"a","endpoint":"10.0.0.1:9000","channelPath":"/c0"}]}`
	if _, err := LoadOwnerMap(write(t, misordered)); err == nil {
		t.Fatal("misordered slot indices should be rejected")
	}

	// slot fields match position -> accepted.
	ordered := `{"epoch":1,"n":2,"slots":[
		{"slot":0,"nodeId":"a","endpoint":"10.0.0.1:9000","channelPath":"/c0"},
		{"slot":1,"nodeId":"b","endpoint":"10.0.0.2:9000","channelPath":"/c1"}]}`
	if _, err := LoadOwnerMap(write(t, ordered)); err != nil {
		t.Fatalf("well-formed owner map rejected: %v", err)
	}
}

func TestIsNoSuchBucketCodeBoundary(t *testing.T) {
	if !isNoSuchBucket(errors.New("api error NoSuchBucket: The specified bucket does not exist")) {
		t.Error("should classify NoSuchBucket")
	}
	if isNoSuchBucket(errors.New("api error NoSuchBucketPolicy: The bucket policy does not exist")) {
		t.Error("must NOT classify NoSuchBucketPolicy as NoSuchBucket")
	}
}
