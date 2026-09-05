package vadb

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

// create your own key first

func (v *IVaDB) get(ctx context.Context, key string) (string, error) {
	cmd := v.cli.B().Get().Key(key).Build()
	res, err := v.cli.Do(ctx, cmd).ToString()
	if err != nil {
		return "", fmt.Errorf("get %q: %w", key, err)
	}
	return res, nil
}

func (v *IVaDB) set(ctx context.Context, key, value string, ttl time.Duration) error {
	var cmd valkey.Completed
	if ttl > 0 {
		cmd = v.cli.B().Set().Key(key).Value(value).Ex(ttl).Build()
	} else {
		cmd = v.cli.B().Set().Key(key).Value(value).Build()
	}
	return v.cli.Do(ctx, cmd).Error()
}

func (v *IVaDB) del(ctx context.Context, key string) error {
	cmd := v.cli.B().Del().Key(key).Build()
	return v.cli.Do(ctx, cmd).Error()
}

// keyExists checks if a key exists in the cache
func (v *IVaDB) keyExists(ctx context.Context, key string) (bool, error) {
	cmd := v.cli.B().Exists().Key(key).Build()
	n, err := v.cli.Do(ctx, cmd).AsInt64()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// keys returns the total number of keys
// patterns can be: * | bucket:*
func (v *IVaDB) keys(ctx context.Context, pattern string, count int64) ([]string, error) {
	var cursor uint64
	var keys []string
	for {
		cmd := v.cli.B().Scan().Cursor(cursor).Match(pattern).Count(count).Build()
		entry, err := v.cli.Do(ctx, cmd).AsScanEntry()
		if err != nil {
			return nil, err
		}
		keys = append(keys, entry.Elements...)
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func (v *IVaDB) totalKeys(ctx context.Context) (int64, error) {
	cmd := v.cli.B().Dbsize().Build()
	return v.cli.Do(ctx, cmd).AsInt64()
}

// end

// unordered or sets

func (v *IVaDB) uoAdd(ctx context.Context, key string, members ...string) error {
	cmd := v.cli.B().Sadd().Key(key).Member(members...).Build()
	return v.cli.Do(ctx, cmd).Error()
}

func (v *IVaDB) uoRem(ctx context.Context, key string, members ...string) error {
	cmd := v.cli.B().Srem().Key(key).Member(members...).Build()
	return v.cli.Do(ctx, cmd).Error()
}

func (v *IVaDB) uoMembers(ctx context.Context, key string) ([]string, error) {
	cmd := v.cli.B().Smembers().Key(key).Build()
	return v.cli.Do(ctx, cmd).AsStrSlice()
}

// end

// hash or map[field value]
// interface usage: key:[field_n:value_n]
// => key:[field_1:value_1,
// ...    field_2:value_2]

func (v *IVaDB) oAdd(ctx context.Context, key, field, value string) error {
	cmd := v.cli.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build()
	return v.cli.Do(ctx, cmd).Error()
}

func (v *IVaDB) oRem(ctx context.Context, key string, field ...string) error {
	cmd := v.cli.B().Hdel().Key(key).Field(field...).Build()
	return v.cli.Do(ctx, cmd).Error()
}

func (v *IVaDB) oMember(ctx context.Context, key, field string) (string, error) {
	cmd := v.cli.B().Hget().Key(key).Field(field).Build()
	return v.cli.Do(ctx, cmd).ToString()
}

func (v *IVaDB) oMembers(ctx context.Context, key string) (map[string]string, error) {
	cmd := v.cli.B().Hgetall().Key(key).Build()
	return v.cli.Do(ctx, cmd).AsStrMap()
}

// end

// list or doubly-linked list

func (v *IVaDB) lAdd(ctx context.Context, key, value string) error {
	cmd := v.cli.B().Lpush().Key(key).Element(value).Build()
	return v.cli.Do(ctx, cmd).Error()
}

func (v *IVaDB) lRem(ctx context.Context, key, value string) error {
	cmd := v.cli.B().Lrem().Key(key).Count(0).Element(value).Build()
	return v.cli.Do(ctx, cmd).Error()
}

func (v *IVaDB) lMember(ctx context.Context, key string) (string, error) {
	cmd := v.cli.B().Lindex().Key(key).Index(0).Build()
	return v.cli.Do(ctx, cmd).ToString()
}

func (v *IVaDB) lMembers(ctx context.Context, key string) ([]string, error) {
	cmd := v.cli.B().Lrange().Key(key).Start(0).Stop(-1).Build()
	return v.cli.Do(ctx, cmd).AsStrSlice()
}

// end
