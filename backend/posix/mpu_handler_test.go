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

package posix

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3response"
)

// fakeMPUHandler records which MPUHandler method Posix dispatched to, so the
// pluggable-strategy seam can be verified without a real backend.
type fakeMPUHandler struct {
	createMPU  bool
	uploadPart bool
	complete   bool
	abort      bool
	listParts  bool
}

// CreateMultipartUpload satisfies the MPUHandler interface, which this PR
// extends for the Af2 write-at-offset backend. Posix.CreateMultipartUpload only
// delegates after an on-disk bucket stat (and, on the generic path, directory
// xattr writes) that a bare &Posix{} cannot satisfy, and it dispatches to this
// method solely via a concrete Af2MPUHandler type assertion — so this records
// dispatch for interface completeness but is not exercised by the bare-Posix
// dispatch assertions below.
func (h *fakeMPUHandler) CreateMultipartUpload(_ context.Context, _ *Posix, _ s3response.CreateMultipartUploadInput, _, _, _ string) (s3response.InitiateMultipartUploadResult, error) {
	h.createMPU = true
	return s3response.InitiateMultipartUploadResult{}, nil
}

func (h *fakeMPUHandler) UploadPart(_ context.Context, _ *Posix, _ *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	h.uploadPart = true
	return &s3.UploadPartOutput{}, nil
}

func (h *fakeMPUHandler) CompleteMultipartUpload(_ context.Context, _ *Posix, _ *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	h.complete = true
	return s3response.CompleteMultipartUploadResult{}, "", nil
}

func (h *fakeMPUHandler) AbortMultipartUpload(_ context.Context, _ *Posix, _ *s3.AbortMultipartUploadInput) error {
	h.abort = true
	return nil
}

func (h *fakeMPUHandler) ListParts(_ context.Context, _ *Posix, _ *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	h.listParts = true
	return s3response.ListPartsResult{}, nil
}

// TestPosixMPUHandlerDispatch asserts the Posix multipart entry points delegate
// to the configured MPUHandler rather than calling a concrete implementation
// directly, so the write-strategy seam introduced here cannot be quietly
// bypassed by a later change.
func TestPosixMPUHandlerDispatch(t *testing.T) {
	h := &fakeMPUHandler{}
	p := &Posix{mpuHandler: h}
	// The action-slot limiter is not initialised on a bare &Posix{}; bypass it.
	ctx := withCtxNoSlot(context.Background())

	if _, err := p.UploadPart(ctx, &s3.UploadPartInput{}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	if !h.uploadPart {
		t.Error("UploadPart did not dispatch through mpuHandler")
	}

	if _, _, err := p.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if !h.complete {
		t.Error("CompleteMultipartUpload did not dispatch through mpuHandler")
	}

	if err := p.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{}); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	if !h.abort {
		t.Error("AbortMultipartUpload did not dispatch through mpuHandler")
	}

	if _, err := p.ListParts(ctx, &s3.ListPartsInput{}); err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if !h.listParts {
		t.Error("ListParts did not dispatch through mpuHandler")
	}
}
