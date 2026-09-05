package vasdk1

import (
	"context"
	"fmt"
	vadb "va/app/core"
)

// Push the data in OBB format
// modes:
// - DLL a doubly linked list
// - MAP a hash map
// - SET a unordered map
func (v *IVaDB) Push(ctx context.Context, mode vadb.DType, p *vadb.IPush) error {
	ps := v.getPerms(ctx, v.id)
	if v.contains(ps, RO) {
		return fmt.Errorf("auth invalid")
	}
	return v.core.Push(ctx, mode, p)
}

// PullBucket returns the bucket branches and the branches data
func (v *IVaDB) PullBucket(ctx context.Context, mode vadb.DType, bucket string) any {
	ps := v.getPerms(ctx, v.id)
	if v.contains(ps, WO) {
		return fmt.Errorf("auth invalid")
	}
	return v.core.PullBucket(ctx, mode, bucket)
}

// PullObject returns the object data
func (v *IVaDB) PullObject(ctx context.Context, mode vadb.DType, bucket, branch, object string) vadb.IPull {
	ps := v.getPerms(ctx, v.id)
	if v.contains(ps, WO) {
		return vadb.IPull{Error: fmt.Errorf("auth invalid")}
	}
	return v.core.PullObject(ctx, mode, bucket, branch, object)
}

// PullBranch retuns the branches object data
func (v *IVaDB) PullBranch(ctx context.Context, mode vadb.DType, bucket, branch string) []vadb.IPull {
	ps := v.getPerms(ctx, v.id)
	if v.contains(ps, WO) {
		return []vadb.IPull{{Error: fmt.Errorf("auth invalid")}}
	}
	return v.core.PullBranch(ctx, mode, bucket, branch)
}

// DeleteObject deletes the object from bucket's branch
func (v *IVaDB) DeleteObject(ctx context.Context, mode vadb.DType, bucket, branch, object string) error {
	ps := v.getPerms(ctx, v.id)
	if v.contains(ps, RO) {
		return fmt.Errorf("auth invalid")
	}
	return v.core.DeleteObject(ctx, mode, bucket, branch, object)
}

// DeleteBranch deletes the bucket's branch and its object
func (v *IVaDB) DeleteBranch(ctx context.Context, mode vadb.DType, bucket, branch, object string) error {
	ps := v.getPerms(ctx, v.id)
	if v.contains(ps, RO) {
		return fmt.Errorf("auth invalid")
	}
	return v.DeleteBranch(ctx, mode, bucket, branch, object)
}

// DeleteBucket deletes the bucket's & its related branch's and its objects
func (v *IVaDB) DeleteBucket(ctx context.Context, mode vadb.DType, bucket string) error {
	ps := v.getPerms(ctx, v.id)
	if v.contains(ps, RO) {
		return fmt.Errorf("auth invalid")
	}
	return v.DeleteBucket(ctx, mode, bucket)
}
