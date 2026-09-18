package vadb

import (
	"context"

	"github.com/valkey-io/valkey-go"
)

func (v *IVaDB) Clear(ctx context.Context, n int64) error {
	return v.clear(ctx, n)
}

func (v *IVaDB) Release(ctx context.Context) error {
	return v.clearAll(ctx)
}

func (v *IVaDB) Cli() valkey.Client {
	return v.cli
}
