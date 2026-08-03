package grant

import (
	"errors"

	"github.com/versity/versitygw/s3err"
)

// ErrGrantMiss means the request carried a valid signature for a known access key,
// but no grant authorizes it on the target bucket (yet). It is a timing gap, not a
// denial: the JFL push or the durable CRDB copy is expected to fill it, so the
// client should back off and retry. It maps to 503 (Retry-After), never 403.
//
// The 403 side of the contract — an unknown access key or a bad signature — is
// terminal and is handled by the gateway's IAM/SigV4 layer, not here, because
// retrying can't help a bad credential. See otter-design-doc.md §14.4.
var ErrGrantMiss = errors.New("otter: no grant for (access key, bucket)")

// SlowDownIfMiss translates a grant-resolution error into the retryable S3 error
// the gateway should return. When err is a grant miss it returns ErrSlowDown (HTTP
// 503 Service Unavailable) and true, so the caller answers "not yet, retry" rather
// than a terminal 4xx. For any other error (e.g. the CRDB read itself failed) it
// returns false and the caller surfaces its own 5xx.
func SlowDownIfMiss(err error) (s3err.APIError, bool) {
	if errors.Is(err, ErrGrantMiss) {
		return s3err.GetAPIError(s3err.ErrSlowDown), true
	}
	return s3err.APIError{}, false
}
