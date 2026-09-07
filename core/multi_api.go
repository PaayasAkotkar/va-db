package vadb

import (
	"context"
	"fmt"
	"time"
)

// Push the data in OBB format
// modes:
// - DLL a doubly linked list
// - MAP a hash map
// - SET a unordered map
func (v *IVaDB) PushBucket(ctx context.Context, mode DType, p *IPush) error {
	if p == nil {
		return fmt.Errorf("push payload is nil")
	}
	k := createKey(mode, p.Bucket, p.Branch, p.Object)
	if err := v.push(ctx, mode, k.bucket, k.branch, k.object, p.Data); err != nil {
		return err
	}
	t := time.Now().Format("03:04PM on 01-02-2006")
	return v.set(ctx, k.guardKey, "guarded@"+t, v.setg.TTL)
}

// PullBucket returns the bucket branches and the branches data
func (v *IVaDB) PullBucket(ctx context.Context, mode DType, bucket string) any {
	k := createKey(mode, bucket, "", "")
	return v.pullBucket(ctx, mode, k.bucket)
}

// PullObject returns the object data
func (v *IVaDB) PullObject(ctx context.Context, mode DType, bucket, branch, object string) IPull {
	if !v.validMode(mode) {
		return IPull{Error: errValidMode}
	}
	return v.pullObject(ctx, mode, bucket, branch, object)
}

// PullBranch retuns the branches object data
func (v *IVaDB) PullBranch(ctx context.Context, mode DType, bucket, branch string) []IPull {
	if !v.validMode(mode) {
		return []IPull{{Error: errValidMode}}
	}
	k := createKey(mode, bucket, branch, "")
	return v.pullBranch(ctx, mode, k.bucket, k.branch)
}

// DeleteObject deletes the object from bucket's branch
func (v *IVaDB) DeleteObject(ctx context.Context, mode DType, bucket, branch, object string) error {
	if !v.validMode(mode) {
		return errValidMode
	}
	k := createKey(mode, bucket, branch, object)
	return v.deleteObject(ctx, mode, k.branch, object, k.object, k.guardKey)
}

// DeleteBranch deletes the bucket's branch and its object
func (v *IVaDB) DeleteBranch(ctx context.Context, mode DType, bucket, branch string) error {
	if !v.validMode(mode) {
		return errValidMode
	}
	k := createKey(mode, bucket, branch, "")
	return v.deleteBranch(ctx, mode, k.bucket, branch, k.branch)
}

// DeleteBucket deletes the bucket's & its related branch's and its objects
func (v *IVaDB) DeleteBucket(ctx context.Context, mode DType, bucket string) error {
	if !v.validMode(mode) {
		return errValidMode
	}
	k := createKey(mode, bucket, "", "")
	return v.deleteBucket(ctx, mode, k.bucket)
}
