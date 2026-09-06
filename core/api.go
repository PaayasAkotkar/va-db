package vadb

import "context"

func (v *IVaDB) Clear(ctx context.Context, n int64) error {
	return v.clear(ctx, n)
}

func (v *IVaDB) Release(ctx context.Context) error {
	return v.clearAll(ctx)
}
