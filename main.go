package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var (
	dialTimeout    = 2 * time.Second
	requestTimeout = 10 * time.Second
)

func main() {

	// cli, kv := initClient()
	// GetSingleValueDemo(ctx, kv)
	// GetMultipleValuesWithPaginationDemo(ctx, kv)
	// LeaseDemo(ctx, cli, kv)

	// ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	// go WatchDemo(ctx, "1")
	// ctx, _ = context.WithTimeout(context.Background(), requestTimeout)
	// go WatchDemo(ctx, "2")
	// ctx, _ = context.WithTimeout(context.Background(), requestTimeout)
	// WatchDemo(ctx, "3")

	go WatchClock("hai")
	go WatchClock("lam")

	select {}
}

func WatchClock(name string) {
	// var name = flag.String("name", "", "give a name")
	flag.Parse()

	cli, _ := initClient()

	// create a sessions to aqcuire a lock
	s, _ := concurrency.NewSession(cli, concurrency.WithTTL(10))
	defer s.Close()
	l := concurrency.NewMutex(s, "key")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// acquire lock (or wait to have it)
	if err := l.TryLock(ctx); err != nil {
		fmt.Println(name, "Trylock arr: ", err.Error())
		return
	}
	fmt.Println("acquired lock for ", name)
	fmt.Println("Do some work in", name)
	time.Sleep(2 * time.Second)
	if err := l.Unlock(ctx); err != nil {
		fmt.Println(err)
	}
	fmt.Println("released lock for ", name)
}

func GetSingleValueDemo(ctx context.Context, kv v3.KV) {
	fmt.Println("*** GetSingleValueDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", v3.WithPrefix())

	// Insert a key value
	pr, err := kv.Put(ctx, "key", "444")
	if err != nil {
		log.Fatal(err)
	}

	rev := pr.Header.Revision

	fmt.Println("Revision:", rev)

	gr, err := kv.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

	// Modify the value of an existing key (create new revision)
	kv.Put(ctx, "key", "555")

	gr, _ = kv.Get(ctx, "key")
	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

	// Get the value of the previous revision
	gr, _ = kv.Get(ctx, "key", v3.WithRev(rev))
	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)
}

func GetMultipleValuesWithPaginationDemo(ctx context.Context, kv v3.KV) {
	fmt.Println("*** GetMultipleValuesWithPaginationDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", v3.WithPrefix())

	// Insert 20 keys
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key_%02d", i)
		kv.Put(ctx, k, strconv.Itoa(i))
	}

	opts := []v3.OpOption{
		v3.WithPrefix(),
		v3.WithSort(v3.SortByKey, v3.SortAscend),
		v3.WithLimit(3),
	}

	gr, _ := kv.Get(ctx, "key", opts...)

	fmt.Println("--- First page ---")
	for _, item := range gr.Kvs {
		fmt.Println(string(item.Key), string(item.Value))
	}

	lastKey := string(gr.Kvs[len(gr.Kvs)-1].Key)

	fmt.Println("--- Second page ---")
	opts = append(opts, v3.WithFromKey())
	gr, _ = kv.Get(ctx, lastKey, opts...)

	// Skipping the first item, which the last item from from the previous Get
	for _, item := range gr.Kvs[1:] {
		fmt.Println(string(item.Key), string(item.Value))
	}
}

func LeaseDemo(ctx context.Context, cli *v3.Client, kv v3.KV) {
	fmt.Println("*** LeaseDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", v3.WithPrefix())

	gr, _ := kv.Get(ctx, "key")
	if len(gr.Kvs) == 0 {
		fmt.Println("No 'key'")
	}

	lease, _ := cli.Grant(ctx, 1)

	// Insert key with a lease of 1 second TTL
	kv.Put(ctx, "key", "value", v3.WithLease(lease.ID))

	gr, _ = kv.Get(ctx, "key")
	if len(gr.Kvs) == 1 {
		fmt.Println("Found 'key'")
	}

	// Let the TTL expire
	time.Sleep(3 * time.Second)

	gr, _ = kv.Get(ctx, "key")
	if len(gr.Kvs) == 0 {
		fmt.Println("No more 'key'")
	}
}

func initClient() (*v3.Client, v3.KV) {
	cli, _ := v3.New((v3.Config{
		DialTimeout: dialTimeout,
		Endpoints:   []string{"127.0.0.1:2379"},
	}))

	// defer cli.Close()
	kv := v3.NewKV(cli)

	return cli, kv
}

func WatchDemo(ctx context.Context, id string) {

	cli, kv := initClient()
	fmt.Println("*** WatchDemo()", id)
	// Delete all keys
	kv.Delete(ctx, "key", v3.WithPrefix())

	stopChan := make(chan interface{})

	go func() {
		watchChan := cli.Watch(ctx, "key", v3.WithPrefix())
		for {
			select {
			case result := <-watchChan:
				for _, ev := range result.Events {
					fmt.Printf("watch %s %s %q : %q\n", id, ev.Type, ev.Kv.Key, ev.Kv.Value)
				}
			case <-stopChan:
				fmt.Println("Done watching.")
				return
			}
		}
	}()

	// Insert some keys
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key_%02d", i)
		kv.Put(ctx, k, strconv.Itoa(i))
	}
	// Make sure watcher go routine has time to recive PUT events
	time.Sleep(5 * time.Second)

	stopChan <- 1

	// Insert some more keys (no one is watching)
	for i := 10; i < 20; i++ {
		k := fmt.Sprintf("key_%02d", i)
		kv.Put(ctx, k, strconv.Itoa(i))
	}
}
