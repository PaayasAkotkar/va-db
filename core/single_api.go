package vadb

import "context"

// PushKey uses the doubly linked list to store in cache
func (v *IVaDB) PushKey(ctx context.Context, key, value string) error {
	return v.lAdd(ctx, key, value)
}

// PullKey returns the elem
func (v *IVaDB) PullKey(ctx context.Context, key string) (string, error) {
	return v.lMember(ctx, key)
}

// DelKey removes the key and its object data
func (v *IVaDB) DelKey(ctx context.Context, key string) error {
	val, err := v.PullKey(ctx, key)
	if err != nil {
		return err
	}
	if err := v.lRem(ctx, key, val); err != nil {
		return err
	}
	return v.del(ctx, key)
}
