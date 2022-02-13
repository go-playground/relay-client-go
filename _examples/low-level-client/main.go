package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-playground/relay-client-go"
	"golang.org/x/sync/semaphore"
)

func main() {

	relayClient, err := relay.New(relay.Config{
		BaseURL: "http://localhost:8080",
	})
	if err != nil {
		panic(err)
	}

	workers := 10
	ch := make(chan *relay.JobHelper)
	sem := semaphore.NewWeighted(int64(workers))
	wg := new(sync.WaitGroup)
	queue := "test-queue"

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(sem, ch)
		}()
	}

	ctx := context.Background()

	for {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			break
		}
		jh, err := relayClient.Next(ctx, queue)
		if err != nil {
			panic(err)
		}
		ch <- jh
	}
	close(ch)
	wg.Wait()
}

func worker(sem *semaphore.Weighted, ch <-chan *relay.JobHelper) {
	for j := range ch {
		process(sem, j)
	}
}

func process(sem *semaphore.Weighted, helper *relay.JobHelper) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func() {
		err := helper.Complete(ctx)
		if err != nil {
			panic(err)
		}
		sem.Release(1)
	}()

	helper.HeartbeatAuto(ctx, time.Second*5)

	var myData string
	_ = json.Unmarshal(helper.Job().Payload, &myData)

	// use myData
}
