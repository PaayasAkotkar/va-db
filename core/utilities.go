package vadb

import (
	"context"
	"fmt"
	"log"
	"strings"
)

type keycreation struct {
	guardKey               string
	bucket, branch, object string
}

func createKey(mode DType, bucket, branch, object string) *keycreation {
	buc := string(mode) + guard + bucket
	bra := buc + guard + branch
	obj := bra + guard + object
	return &keycreation{
		guardKey: obj + guard + "guard",
		bucket:   buc,
		branch:   bra,
		object:   obj,
	}
}

func normalizeKey(mode DType, guardKey string) *keycreation {
	parts := strings.Split(guardKey, guard)
	if len(parts) != 5 || parts[0] != string(mode) || parts[4] != "guard" {
		return &keycreation{}
	}

	return &keycreation{
		guardKey: guardKey,
		bucket:   parts[1],
		branch:   parts[2],
		object:   parts[3],
	}
}

// push

func (v *IVaDB) push(ctx context.Context, mode DType, bucket, branch, object, data string) error {

	switch mode {
	case DLL:
		if err := v.lAdd(ctx, bucket, branch); err != nil {
			return err
		}
		if err := v.lAdd(ctx, branch, object); err != nil {
			return err
		}
	case MAP:
		if err := v.oAdd(ctx, bucket, branch, branch); err != nil {
			return err
		}
		if err := v.oAdd(ctx, branch, object, object); err != nil {
			return err
		}
	case SET:
		if err := v.uoAdd(ctx, bucket, branch); err != nil {
			return err
		}
		if err := v.uoAdd(ctx, branch, object); err != nil {
			return err
		}
	default:
		return fmt.Errorf("mode not matched")
	}
	if err := v.set(ctx, object, data, v.setg.TTL); err != nil {
		return err
	}
	return nil
}

// end

// pull

type IPull struct {
	Bucket, Branch, Object string
	Data                   string
	Fresh                  bool
	Error                  error
	Mode                   DType
}

func (v *IVaDB) pullObject(ctx context.Context, mode DType, bucket, branch, object string) IPull {
	k := createKey(mode, bucket, branch, object)
	n := normalizeKey(mode, k.guardKey)

	fresh, err := v.keyExists(ctx, k.guardKey)
	if err != nil {
		return IPull{Bucket: n.bucket, Branch: n.branch, Object: n.object, Fresh: fresh, Error: err, Mode: mode}
	}

	data, err := v.get(ctx, k.object)

	return IPull{Bucket: n.bucket, Branch: n.branch, Object: n.object, Data: data, Fresh: fresh, Error: err, Mode: mode}
}

func (v *IVaDB) pullBranch(ctx context.Context, mode DType, bucket, branch string) []IPull {
	objects, err := v.objectNames(ctx, mode, branch)
	if err != nil {
		return []IPull{{Error: err, Mode: mode}}
	}
	results := make([]IPull, 0, len(objects))
	for _, obj := range objects {
		results = append(results, v.pullObject(ctx, mode, bucket, branch, obj))
	}
	return results
}

func (v *IVaDB) pullBucket(ctx context.Context, mode DType, bucket string) [][]IPull {
	bras, err := v.branchNames(ctx, mode, bucket)
	if err != nil {
		log.Println(err)
		return nil
	}

	p := make([][]IPull, 0, len(bras))
	for _, bra := range bras {
		_p := v.pullBranch(ctx, mode, bucket, bra)
		p = append(p, _p)
	}
	return p
}

// end

// del

func (v *IVaDB) deleteObject(ctx context.Context, mode DType, branchKey, objectName, objectKey, guardKey string) error {
	var err error
	switch mode {
	case DLL:
		err = v.lRem(ctx, branchKey, objectName)
	case SET:
		err = v.uoRem(ctx, branchKey, objectName)
	case MAP:
		err = v.oRem(ctx, branchKey, objectName)
	default:
		return fmt.Errorf("mode not matched")
	}
	if err != nil {
		return err
	}
	if err := v.del(ctx, guardKey); err != nil {
		return err
	}
	return v.del(ctx, objectKey)

}

func (v *IVaDB) deleteBranch(ctx context.Context, mode DType, bucket, branchName, branchKey string) error {
	objects, err := v.objectNames(ctx, mode, branchKey)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		k := createKey(mode, strings.TrimPrefix(bucket, string(mode)+guard), branchName, obj)
		if err := v.del(ctx, k.guardKey); err != nil {
			log.Println(err)
		}
		if err := v.del(ctx, k.object); err != nil {
			log.Println(err)
		}
	}
	if err := v.del(ctx, branchKey); err != nil {
		return err
	}

	switch mode {
	case DLL:
		return v.lRem(ctx, bucket, branchName)
	case MAP:
		return v.oRem(ctx, bucket, branchName)
	case SET:
		return v.uoRem(ctx, bucket, branchName)
	default:
		return fmt.Errorf("mode not matched: %v", mode)
	}
}

func (v *IVaDB) deleteBucket(ctx context.Context, mode DType, bucket string) error {
	branches, err := v.branchNames(ctx, mode, bucket)
	if err != nil {
		return err
	}
	bucketName := strings.TrimPrefix(bucket, string(mode)+guard)
	for _, branch := range branches {
		branchKey := createKey(mode, bucketName, branch, "").branch
		if err := v.deleteBranch(ctx, mode, bucket, branch, branchKey); err != nil {
			return err
		}
	}
	return v.del(ctx, bucket)
}

func (v *IVaDB) clear(ctx context.Context, count int64) error {
	ks, err := v.keys(ctx, "*", count)

	if err != nil {
		return err
	}

	var err_ strings.Builder
	for _, r := range ks {
		if err := v.del(ctx, r); err != nil {
			err_.WriteString(err.Error())
			err_.WriteString("; ")
		}
	}
	if err_.Len() == 0 {
		return nil
	}
	return fmt.Errorf("%s", err_.String())
}

func (v *IVaDB) clearAll(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	count, err := v.totalKeys(ctx)
	if err != nil {
		return err
	}

	return v.clear(ctx, count)
}

// end

// ultilies

func (v *IVaDB) validMode(mode DType) bool {
	switch mode {
	case DLL, MAP, SET:
		return true
	default:
		return false
	}
}

func (v *IVaDB) branchNames(ctx context.Context, mode DType, bucket string) ([]string, error) {
	switch mode {
	case DLL:
		return v.lMembers(ctx, bucket)
	case SET:
		return v.uoMembers(ctx, bucket)
	case MAP:
		m, err := v.oMembers(ctx, bucket)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(m))
		for f := range m {
			names = append(names, f)
		}
		return names, nil
	default:
		return nil, fmt.Errorf("mode not matched: %v", mode)
	}
}
func (v *IVaDB) objectNames(ctx context.Context, mode DType, branch string) ([]string, error) {
	switch mode {
	case DLL:
		return v.lMembers(ctx, branch)
	case SET:
		return v.uoMembers(ctx, branch)
	case MAP:
		m, err := v.oMembers(ctx, branch)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(m))
		for f := range m {
			names = append(names, f)
		}
		return names, nil
	default:
		return nil, fmt.Errorf("mode not matched: %v", mode)
	}
}

// end
