package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-playground/relay-client-go"
	"github.com/go-playground/relay-client-go/consumer"
)

var (
	processed uint64
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	ctx := context.Background()

	client, err := relay.New(relay.Config{
		BaseURL: "http://127.0.0.1:8080",
	})
	if err != nil {
		panic(err)
	}

	queue := "test-queue"

	c, err := consumer.New(consumer.Config{
		Workers:   20,
		Pollers:   3,
		Client:    client,
		Processor: new(processor),
		Queue:     queue,
	})
	if err != nil {
		panic(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// report counts
		for {
			time.Sleep(time.Second)
			oldProcessed := atomic.SwapUint64(&processed, 0)
			fmt.Println("Processed ", oldProcessed)
		}
	}()

	err = c.Start(ctx)
	if err != nil {
		panic(err)
	}

	wg.Wait()
}

var _ consumer.Processor = (*processor)(nil)

type processor struct {
}

func (p processor) Process(ctx context.Context, helper *relay.JobHelper) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	atomic.AddUint64(&processed, 1)
	return nil
}
