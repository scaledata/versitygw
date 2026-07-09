// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package posix

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3response"
)

// chooseMPUHandler returns the right MPUHandler for the given configuration.
func chooseMPUHandler(sameDirTmp bool) MPUHandler {
	if sameDirTmp {
		return Af2MPUHandler{}
	}
	return StandardMPUHandler{}
}

// MPUHandler abstracts the multipart upload write strategy for a Posix backend.
// The standard implementation stages one file per part and concatenates at
// Complete. Alternative implementations can plug in different strategies — for
// example the AF2 implementation writes every part directly at its final byte
// offset in a single data file and reveals it with a same-directory rename,
// so each byte is written exactly once with no staging copy.
//
// Methods receive *Posix so they can call back to shared helpers (tmpDir,
// meta, checkUploadIDExists, etc.). Both the interface and all concrete
// implementations live in this package, so the *Posix parameter does not
// create an import cycle.
type MPUHandler interface {
	// CreateMultipartUpload initialises per-upload state after the common
	// directory setup. acquireActionSlot is handled by Posix.CreateMultipartUpload.
	CreateMultipartUpload(ctx context.Context, p *Posix, mpu s3response.CreateMultipartUploadInput, bucket, object, uploadID string) (s3response.InitiateMultipartUploadResult, error)

	// UploadPart handles the write side of a part upload.
	// acquireActionSlot is handled by Posix.UploadPart before calling this.
	UploadPart(ctx context.Context, p *Posix, input *s3.UploadPartInput) (*s3.UploadPartOutput, error)

	// CompleteMultipartUpload assembles and reveals the final object.
	// acquireActionSlot is handled by Posix.CompleteMultipartUpload before calling this.
	CompleteMultipartUpload(ctx context.Context, p *Posix, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error)

	// AbortMultipartUpload cleans up staging state for a cancelled upload.
	// acquireActionSlot is handled by Posix.AbortMultipartUpload before calling this.
	AbortMultipartUpload(ctx context.Context, p *Posix, input *s3.AbortMultipartUploadInput) error

	// ListParts returns the recorded parts for an in-flight upload.
	// acquireActionSlot is handled by Posix.ListParts before calling this.
	ListParts(ctx context.Context, p *Posix, input *s3.ListPartsInput) (s3response.ListPartsResult, error)
}

// StandardMPUHandler is the default MPUHandler for a generic POSIX filesystem.
// It delegates to the existing Posix staging-and-concat methods unchanged.
type StandardMPUHandler struct{}

func (StandardMPUHandler) CreateMultipartUpload(_ context.Context, _ *Posix, _ s3response.CreateMultipartUploadInput, bucket, object, uploadID string) (s3response.InitiateMultipartUploadResult, error) {
	return s3response.InitiateMultipartUploadResult{Bucket: bucket, Key: object, UploadId: uploadID}, nil
}

func (StandardMPUHandler) UploadPart(ctx context.Context, p *Posix, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	return p.UploadPartWithPostFunc(ctx, input, func(*os.File) error { return nil })
}

func (StandardMPUHandler) CompleteMultipartUpload(ctx context.Context, p *Posix, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	return p.CompleteMultipartUploadWithCopy(ctx, input, nil)
}

func (StandardMPUHandler) AbortMultipartUpload(ctx context.Context, p *Posix, input *s3.AbortMultipartUploadInput) error {
	return p.abortMultipartInternal(ctx, input)
}

func (StandardMPUHandler) ListParts(ctx context.Context, p *Posix, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	return p.listPartsInternal(ctx, input)
}
