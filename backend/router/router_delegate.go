package router

import (
	"bufio"
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3response"
)

// The methods below delegate to the current local backend via the atomic cfg
// snapshot. They cover all backend.Backend methods that Router does not
// override with its own routing logic.

func (r *Router) Shutdown() { r.cfg.Load().local.Shutdown() }

func (r *Router) NormalizeObjectKey(bucket, object string) string {
	return r.cfg.Load().local.NormalizeObjectKey(bucket, object)
}

func (r *Router) ListBuckets(ctx context.Context, in s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	return r.cfg.Load().local.ListBuckets(ctx, in)
}

func (r *Router) HeadBucket(ctx context.Context, in *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	return r.cfg.Load().local.HeadBucket(ctx, in)
}

func (r *Router) GetBucketAcl(ctx context.Context, in *s3.GetBucketAclInput) ([]byte, error) {
	return r.cfg.Load().local.GetBucketAcl(ctx, in)
}

func (r *Router) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	return r.cfg.Load().local.PutBucketAcl(ctx, bucket, data)
}

func (r *Router) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	return r.cfg.Load().local.PutBucketVersioning(ctx, bucket, status)
}

func (r *Router) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	return r.cfg.Load().local.GetBucketVersioning(ctx, bucket)
}

func (r *Router) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	return r.cfg.Load().local.PutBucketPolicy(ctx, bucket, policy)
}

func (r *Router) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	return r.cfg.Load().local.GetBucketPolicy(ctx, bucket)
}

func (r *Router) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return r.cfg.Load().local.DeleteBucketPolicy(ctx, bucket)
}

func (r *Router) PutBucketOwnershipControls(ctx context.Context, bucket string, ownership types.ObjectOwnership) error {
	return r.cfg.Load().local.PutBucketOwnershipControls(ctx, bucket, ownership)
}

func (r *Router) GetBucketOwnershipControls(ctx context.Context, bucket string) (types.ObjectOwnership, error) {
	return r.cfg.Load().local.GetBucketOwnershipControls(ctx, bucket)
}

func (r *Router) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	return r.cfg.Load().local.DeleteBucketOwnershipControls(ctx, bucket)
}

func (r *Router) PutBucketCors(ctx context.Context, bucket string, cors []byte) error {
	return r.cfg.Load().local.PutBucketCors(ctx, bucket, cors)
}

func (r *Router) GetBucketCors(ctx context.Context, bucket string) ([]byte, error) {
	return r.cfg.Load().local.GetBucketCors(ctx, bucket)
}

func (r *Router) DeleteBucketCors(ctx context.Context, bucket string) error {
	return r.cfg.Load().local.DeleteBucketCors(ctx, bucket)
}

func (r *Router) PutBucketWebsite(ctx context.Context, bucket string, website []byte) error {
	return r.cfg.Load().local.PutBucketWebsite(ctx, bucket, website)
}

func (r *Router) GetBucketWebsite(ctx context.Context, bucket string) ([]byte, error) {
	return r.cfg.Load().local.GetBucketWebsite(ctx, bucket)
}

func (r *Router) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	return r.cfg.Load().local.DeleteBucketWebsite(ctx, bucket)
}

func (r *Router) ListMultipartUploads(ctx context.Context, in *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	return r.cfg.Load().local.ListMultipartUploads(ctx, in)
}

// ListObjects and ListObjectsV2 are NOT delegated here: the Router overrides
// them with a cross-channel fan-out + k-way merge (see list.go), so a
// single-endpoint LIST enumerates the whole bucket rather than this node's
// ~1/N channel.

func (r *Router) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	return r.cfg.Load().local.DeleteObjects(ctx, in)
}

func (r *Router) ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	return r.cfg.Load().local.ListObjectVersions(ctx, in)
}

func (r *Router) SelectObjectContent(ctx context.Context, in *s3.SelectObjectContentInput) func(w *bufio.Writer) {
	return r.cfg.Load().local.SelectObjectContent(ctx, in)
}

func (r *Router) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	return r.cfg.Load().local.GetBucketTagging(ctx, bucket)
}

func (r *Router) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	return r.cfg.Load().local.PutBucketTagging(ctx, bucket, tags)
}

func (r *Router) DeleteBucketTagging(ctx context.Context, bucket string) error {
	return r.cfg.Load().local.DeleteBucketTagging(ctx, bucket)
}

func (r *Router) PutObjectLockConfiguration(ctx context.Context, bucket string, config []byte) error {
	return r.cfg.Load().local.PutObjectLockConfiguration(ctx, bucket, config)
}

func (r *Router) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	return r.cfg.Load().local.GetObjectLockConfiguration(ctx, bucket)
}

func (r *Router) ChangeBucketOwner(ctx context.Context, bucket, owner string) error {
	return r.cfg.Load().local.ChangeBucketOwner(ctx, bucket, owner)
}

func (r *Router) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	return r.cfg.Load().local.ListBucketsAndOwners(ctx)
}
