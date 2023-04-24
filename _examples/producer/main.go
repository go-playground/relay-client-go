package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-playground/errors/v5"
	"github.com/go-playground/relay-client-go"
	"github.com/go-playground/relay-client-go/producer"
)

var (
	enqueued uint64
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1024
	http.DefaultTransport.(*http.Transport).MaxConnsPerHost = 1024
}

type payload struct {
	Key string `json:"key"`
}

func main() {
	ctx := context.Background()

	client, err := relay.New[payload, struct{}](relay.Config{
		BaseURL: "http://127.0.0.1:8080",
	})
	if err != nil {
		panic(err)
	}

	queue := "test-queue"

	p, err := producer.New(producer.Config[payload, struct{}, *relay.Client[payload, struct{}]]{
		Enqueuer: client,
	})
	if err != nil {
		panic(err)
	}

	wg := new(sync.WaitGroup)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				err := p.Enqueue(ctx, relay.Job[payload, struct{}]{
					ID:         strconv.Itoa(rand.Intn(1_000_000_000)),
					Queue:      queue,
					Timeout:    30,
					MaxRetries: 0,
					Payload:    payload{Key: "I'm a little teapot"},
				})
				if err != nil {
					var e relay.ErrJobExits
					if errors.As(err, &e) {
						continue
					}
					panic("enqueue" + err.Error())
				}
				atomic.AddUint64(&enqueued, 1)
			}
		}()
	}

	// report counts
	for {
		time.Sleep(time.Second)
		oldEnqueued := atomic.SwapUint64(&enqueued, 0)
		fmt.Println("Enqueued:", oldEnqueued)
	}
	wg.Wait()
}
