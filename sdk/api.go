package vasdk1

import (
	"context"
	"log"
	vadb "va/app/core"
)

func (v *IVaDB) TSubscribe(
	ctx context.Context,
	n int, // number of outputs
	mode vadb.DType,
	bucket string,
	branch string,
	object string,
) chan *vadb.IPull {
	out := make(chan *vadb.IPull, n)

	channel := createKey(
		mode,
		bucket,
		branch,
		object,
	)

	messages, errors := v.subscribeChannel(ctx, channel)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return

			case err, ok := <-errors:
				if ok && err != nil {
					log.Println("valkey subscription:", err)
				}
				return

			case msg, ok := <-messages:
				if !ok {
					return
				}

				// Pull happens internally.
				//				result := v.core.PullObject(
				//					ctx,
				//					mode,
				//					bucket,
				//					branch,
				//					object,
				//				)
				//
				result := &vadb.IPull{
					Bucket: bucket,
					Branch: branch,
					Object: object,
					Data:   msg,
					Fresh:  true,
					Mode:   mode,
					Error:  nil,
				}
				select {
				case out <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

// TPublish the data in OBB format
// modes:
// - DLL a doubly linked list
// - MAP a hash map
// - SET a unordered map
func (v *IVaDB) TPublish(
	ctx context.Context,
	mode vadb.DType,
	push *vadb.IPush,
) {
	if push == nil {
		return
	}

	if err := v.core.PushBucket(ctx, mode, push); err != nil {
		log.Println("push bucket:", err)
		return
	}

	channel := createKey(
		mode,
		push.Bucket,
		push.Branch,
		push.Object,
	)

	if err := v.createChannel(
		ctx,
		channel,
		push.Data,
	); err != nil {
		log.Println("publish notification:", err)
	}
}

func (v *IVaDB) Subscribe(ctx context.Context, n int, key string) chan *string {
	d := make(chan *string, n)
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

func (v *IVaDB) ExplicitUnsubscribeKey(ctx context.Context, key string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	ch := make(chan error, 1)
	go func() {
		ch <- v.core.DelKey(ctx, key)
	}()
	select {
	case t := <-ch:
		return t
	case <-ctx.Done():
		return nil
	}
}

func (v *IVaDB) ExplicitRemoveObject(ctx context.Context, mode vadb.DType, bucket, branch, object string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	ch := make(chan error, 1)
	go func() {
		if err := v.core.DeleteObject(ctx, mode, bucket, branch, object); err != nil {
			ch <- err
		}
		ch <- nil
	}()
	select {
	case t := <-ch:
		return t
	case <-ctx.Done():
		return nil
	}
}
func (v *IVaDB) ExplicitRemoveBranch(ctx context.Context, mode vadb.DType, bucket, branch string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	ch := make(chan error, 1)
	go func() {
		if err := v.core.DeleteBranch(ctx, mode, bucket, branch); err != nil {
			ch <- err
		}
		ch <- nil
	}()
	select {
	case t := <-ch:
		return t
	case <-ctx.Done():
		return nil
	}
}
func (v *IVaDB) ExplicitRemoveBucket(ctx context.Context, mode vadb.DType, bucket string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	ch := make(chan error, 1)
	go func() {
		if err := v.core.DeleteBucket(ctx, mode, bucket); err != nil {
			ch <- err
		}
		ch <- nil
	}()
	select {
	case t := <-ch:
		return t
	case <-ctx.Done():
		return nil
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
