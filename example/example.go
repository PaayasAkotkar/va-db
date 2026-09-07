package example

import (
	"context"
	"log"
	"sync"
	"time"
	vadb "va/app/core"
	vasdk1 "va/app/sdk"

	"github.com/valkey-io/valkey-go"
)

func Mixture() {
	ctx := context.Background()

	port := "127.0.0.1:6379"
	c, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{port},
	})
	if err != nil {
		panic(err)
	}
	va := vadb.New(vadb.IConfig{Cli: c,
		Set: &vadb.ISettings{
			TTL: time.Second * 12,
		},
	})
	key := "test_key_list"
	if err := va.PushKey(ctx, key, "succeed"); err != nil {
		panic(err)
	}
	v, err := va.PullKey(ctx, key)
	if err != nil {
		panic(err)
	}
	log.Println("val: ", v)

	for _, mode := range []vadb.DType{vadb.DLL, vadb.MAP, vadb.SET} {
		bucket := "example_" + string(mode)
		branch := "branch_a"
		secondBranch := "branch_b"
		if err := va.DeleteBucket(ctx, mode, bucket); err != nil {
			panic(err)
		}
		for _, item := range []vadb.IPush{
			{Bucket: bucket, Branch: branch, Object: "object_1", Data: "value_1"},
			{Bucket: bucket, Branch: branch, Object: "object_2", Data: "value_2"},
			{Bucket: bucket, Branch: secondBranch, Object: "object_3", Data: "value_3"},
		} {
			if err := va.PushBucket(ctx, mode, &item); err != nil {
				panic(err)
			}
		}
		log.Println(mode, "object:", va.PullObject(ctx, mode, bucket, branch, "object_1"))
		log.Println(mode, "branch:", va.PullBranch(ctx, mode, bucket, branch))
		log.Println(mode, "bucket:", va.PullBucket(ctx, mode, bucket))

		if err := va.DeleteObject(ctx, mode, bucket, branch, "object_1"); err != nil {
			panic(err)
		}
		if err := va.DeleteBranch(ctx, mode, bucket, secondBranch); err != nil {
			panic(err)
		}
		log.Println(mode, "after deletes:", va.PullBucket(ctx, mode, bucket))
		if err := va.DeleteBucket(ctx, mode, bucket); err != nil {
			panic(err)
		}
	}
}

func KeyPubSub() {
	ctx := context.Background()

	port := "127.0.0.1:6379"
	c, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{port},
	})
	if err != nil {
		panic(err)
	}
	va := vasdk1.New(vadb.IConfig{Cli: c,
		Set: &vadb.ISettings{
			TTL: time.Second * 12,
		},
	}, 10)
	log.Println("succeed connection 🤗")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		data := va.Subscribe(ctx, "jolly")
		select {
		case d := <-data:
			log.Println("pulled data from valkey: ", *d)
		default:
			log.Println("waiting...")
		}
	}()
	time.Sleep(2 * time.Second)
	go func() {
		defer wg.Done()
		va.Publish(ctx, "jolly", "12")
	}()
	wg.Wait()
}

func TreePubSub() {
	ctx := context.Background()

	port := "127.0.0.1:6379"
	c, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{port},
	})
	if err != nil {
		panic(err)
	}
	va := vasdk1.New(vadb.IConfig{Cli: c,
		Set: &vadb.ISettings{
			TTL: time.Second * 12,
		},
	}, 10)
	log.Println("succeed connection 🤗")
	bucket, branch, object := "ps5", "games", "gta-vi"
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		data := va.TSubscribe(ctx, vadb.MAP, bucket, branch, object)
		select {
		case d := <-data:
			log.Println("pulled data from valkey: ", *d)
		default:
			log.Println("waiting...")
		}
	}()
	time.Sleep(2 * time.Second)
	go func() {
		defer wg.Done()
		va.TPublish(ctx, vadb.MAP, &vadb.IPush{
			Bucket: bucket,
			Branch: branch,
			Object: object,
			Data:   "nov 19 2026",
		})
	}()
	wg.Wait()
}

func AsyncPubSub() {
	ctx := context.Background()

	port := "127.0.0.1:6379"
	c, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{port},
	})
	if err != nil {
		panic(err)
	}
	va := vasdk1.New(vadb.IConfig{Cli: c,
		Set: &vadb.ISettings{
			TTL: time.Second * 12,
		},
	}, 10)
	log.Println("succeed connection 🤗")
	bucket, branch, object := "ps5", "games", "gta-vi"
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		data := va.TSubscribe(ctx, vadb.MAP, bucket, branch, object)
		select {
		case d := <-data:
			log.Println("pulled data from valkey: ", *d)
		default:
			log.Println("waiting...")
		}
	}()
	go func() {
		defer wg.Done()
		data := va.Subscribe(ctx, bucket)
		select {
		case d := <-data:
			log.Println("pulled data from valkey: ", *d)
		default:
			log.Println("waiting...")
		}
	}()
	time.Sleep(2 * time.Second)
	go func() {
		defer wg.Done()
		va.TPublish(ctx, vadb.MAP, &vadb.IPush{
			Bucket: bucket,
			Branch: branch,
			Object: object,
			Data:   "via tree: nov 19 2026",
		})
	}()

	go func() {
		defer wg.Done()
		va.Publish(ctx, bucket, "via key: nov 19 2026")
	}()

	wg.Wait()
}
