// Copyright 2023 Versity Software
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

package middlewares

import (
	"bytes"
	"io"

	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/s3api/utils"
)

// ChecksumReader, NewChecksumReader, and MockChecksumReader live in
// s3api/utils (see checksum-reader-iface.go) since they have no Fiber
// dependency of their own, unlike wrapBodyReader below. Embedders that
// need the interface for a type assertion don't need to compile in Fiber
// just to reach it.

func wrapBodyReader(ctx fiber.Ctx, wr func(io.Reader) io.Reader) {
	rdr, ok := utils.ContextKeyBodyReader.Get(ctx).(io.Reader)
	if !ok {
		rdr = ctx.Request().BodyStream()
		// Override the body reader with an empty reader to prevent panics
		// in case of unexpected or malformed HTTP requests.
		if rdr == nil {
			rdr = bytes.NewBuffer([]byte{})
		}
	}

	r := wr(rdr)
	// Ensure checksum behavior is stacked if the original body reader had it.
	r = utils.NewChecksumReader(rdr, r)

	utils.ContextKeyBodyReader.Set(ctx, r)
}
