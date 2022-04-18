package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-playground/relay-client-go"
	"golang.org/x/sync/semaphore"
)

type payload struct {
	Key string `json:"key"`
}

func main() {

	relayClient, err := relay.New[payload, struct{}](relay.Config{
		BaseURL: "http://localhost:8080",
	})
	if err != nil {
		panic(err)
	}

	workers := 10
	ch := make(chan *relay.JobHelper[payload, struct{}])
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
		jh, err := relayClient.Next(ctx, queue, 1)
		if err != nil {
			panic(err)
		}
		ch <- jh[0]
	}
	close(ch)
	wg.Wait()
}

func worker(sem *semaphore.Weighted, ch <-chan *relay.JobHelper[payload, struct{}]) {
	for j := range ch {
		process(sem, j)
	}
}

func process(sem *semaphore.Weighted, helper *relay.JobHelper[payload, struct{}]) {
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

	pl := helper.Job().Payload

	// use payload(pl)
	fmt.Println(pl)
}
