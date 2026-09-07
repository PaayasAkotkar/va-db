package vasdk1

import (
	"context"
	"log"
	vadb "va/app/core"
)

//// Push the data in OBB format
//// modes:
//// - DLL a doubly linked list
//// - MAP a hash map
//// - SET a unordered map
//func (v *IVaDB) Push(ctx context.Context, mode vadb.DType, p *vadb.IPush) error {
//	ps := v.getPerms(ctx, v.id)
//	if v.contains(ps, RO) {
//		return fmt.Errorf("auth invalid")
//	}
//	return v.core.Push(ctx, mode, p)
//}
//
//// PullBucket returns the bucket branches and the branches data
//func (v *IVaDB) PullBucket(ctx context.Context, mode vadb.DType, bucket string) any {
//	ps := v.getPerms(ctx, v.id)
//	if v.contains(ps, WO) {
//		return fmt.Errorf("auth invalid")
//	}
//	return v.core.PullBucket(ctx, mode, bucket)
//}
//
//// PullObject returns the object data
//func (v *IVaDB) PullObject(ctx context.Context, mode vadb.DType, bucket, branch, object string) vadb.IPull {
//	ps := v.getPerms(ctx, v.id)
//	if v.contains(ps, WO) {
//		return vadb.IPull{Error: fmt.Errorf("auth invalid")}
//	}
//	return v.core.PullObject(ctx, mode, bucket, branch, object)
//}
//
//// PullBranch retuns the branches object data
//func (v *IVaDB) PullBranch(ctx context.Context, mode vadb.DType, bucket, branch string) []vadb.IPull {
//	ps := v.getPerms(ctx, v.id)
//	if v.contains(ps, WO) {
//		return []vadb.IPull{{Error: fmt.Errorf("auth invalid")}}
//	}
//	return v.core.PullBranch(ctx, mode, bucket, branch)
//}
//
//// DeleteObject deletes the object from bucket's branch
//func (v *IVaDB) DeleteObject(ctx context.Context, mode vadb.DType, bucket, branch, object string) error {
//	ps := v.getPerms(ctx, v.id)
//	if v.contains(ps, RO) {
//		return fmt.Errorf("auth invalid")
//	}
//	return v.core.DeleteObject(ctx, mode, bucket, branch, object)
//}
//
//// DeleteBranch deletes the bucket's branch and its object
//func (v *IVaDB) DeleteBranch(ctx context.Context, mode vadb.DType, bucket, branch, object string) error {
//	ps := v.getPerms(ctx, v.id)
//	if v.contains(ps, RO) {
//		return fmt.Errorf("auth invalid")
//	}
//	return v.DeleteBranch(ctx, mode, bucket, branch, object)
//}
//
//// DeleteBucket deletes the bucket's & its related branch's and its objects
//func (v *IVaDB) DeleteBucket(ctx context.Context, mode vadb.DType, bucket string) error {
//	ps := v.getPerms(ctx, v.id)
//	if v.contains(ps, RO) {
//		return fmt.Errorf("auth invalid")
//	}
//	return v.DeleteBucket(ctx, mode, bucket)
//}

func (v *IVaDB) TSubscribe(ctx context.Context, mode vadb.DType, bucket, branch, object string) chan *vadb.IPull {
	d := make(chan *vadb.IPull, 1)
	select {
	case ch, ok := <-v.happen:
		if ok && ch {
			a := v.core.PullObject(ctx, mode, bucket, branch, object)
			d <- &a
		}
	case <-ctx.Done():
		v.Unsubscribe(ctx)
	}
	return d
}

// TPublish the data in OBB format
// modes:
// - DLL a doubly linked list
// - MAP a hash map
// - SET a unordered map
func (v *IVaDB) TPublish(ctx context.Context, mode vadb.DType, p *vadb.IPush) {
	if err := v.core.PushBucket(ctx, mode, p); err != nil {
		log.Println(err)
		return
	}
	select {
	case v.happen <- true:
	case <-ctx.Done():
		v.Unsubscribe(ctx)
	}
}

func (v *IVaDB) Subscribe(ctx context.Context, key string) chan *string {
	d := make(chan *string, 1)
	select {
	case ch, ok := <-v.happen:
		if ok && ch {
			a, err := v.core.PullKey(ctx, key)
			if err != nil {
				log.Println(err)
				return d
			}
			d <- &a
		}
	case <-ctx.Done():
		v.Unsubscribe(ctx)
	}
	return d
}

func (v *IVaDB) Publish(ctx context.Context, key, value string) {
	if err := v.core.PushKey(ctx, key, value); err != nil {
		log.Println(err)
		return
	}
	select {
	case v.happen <- true:
	case <-ctx.Done():
		v.Unsubscribe(ctx)
	}
}

func (v *IVaDB) Unsubscribe(ctx context.Context) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if _, ok := <-v.happen; ok {
		close(v.happen)
	}
	if err := v.core.Release(ctx); err != nil {
		log.Println(err)
	}
}
