package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-playground/backoff-sys"
	"github.com/go-playground/errors/v5"
	errorsext "github.com/go-playground/pkg/v5/errors"
	"github.com/go-playground/relay-client-go"
	"golang.org/x/sync/errgroup"
)

// Processor represents a processor of Jobs
type Processor[P any, S any] interface {
	// Process ...  Warning if returning an error it better be a fatal operational error
	Process(context.Context, *relay.JobHelper[P, S]) error
}

// Config contains all configuration data to initialize a Consumer for use.
type Config[P any, S any, T Processor[P, S]] struct {

	// Workers represent the maximum number of Jobs that can be in-flight at one time by having a
	// maximum number of workers.
	// Default will be set to one.
	Workers int

	// Pollers indicate the maximum number of polling workers trying to retrieve Jobs for processing.
	// this should only be tuned for consistent high-flow services for which a single poller becomes the
	// bottleneck which is rare. This should never be set greater than the maximum number of workers and in
	// most cases should be far less than.
	// By default, there will only be one poller which should be all you need 99.99999% of the time.
	Pollers int

	// NoAutoComplete turns off auto-completion of a Job that is processed without error allowing
	// the Processor full control to complete or not as necessary.
	NoAutoComplete bool

	// Processor is the main processor of Jobs.
	Processor T

	// Queue is the Jbo Queue for which to pull jobs from for processing.
	Queue string

	// Client represents the pre-configured low-level relay client.
	Client *relay.Client[P, S]

	// Backoff if the backoff used when calling the `next` or `complete` endpoint and there is no data yet
	// available.
	// Optional: If not set a default backoff is used.
	Backoff backoff.Exponential
}

// Consumer is a wrapper around the low-level Relay Client to abstract away polling and distribution of Jobs
// for processing.
type Consumer[P any, S any, T Processor[P, S]] struct {
	processor    T
	sem          chan struct{}
	workers      int
	pollers      int
	queue        string
	client       *relay.Client[P, S]
	bo           backoff.Exponential
	autoComplete bool
}

// New initializes a new Consumer for use.
func New[P any, S any, T Processor[P, S]](cfg Config[P, S, T]) (*Consumer[P, S, T], error) {
	if cfg.Queue == "" {
		return nil, errors.New("Queue required")
	}
	if cfg.Client == nil {
		return nil, errors.New("Client required")
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Pollers < 1 {
		cfg.Pollers = 1
	}

	if cfg.Pollers > cfg.Workers {
		return nil, errors.New("Pollers should never be greater than the number of workers.")
	}

	defaultBackoff := backoff.Exponential{}
	if cfg.Backoff == defaultBackoff {
		cfg.Backoff = backoff.NewExponential().Interval(time.Millisecond * 100).Jitter(time.Millisecond * 25).Max(time.Second).Init()
	}

	return &Consumer[P, S, T]{
		processor:    cfg.Processor,
		sem:          make(chan struct{}, cfg.Workers),
		workers:      cfg.Workers,
		pollers:      cfg.Pollers,
		queue:        cfg.Queue,
		client:       cfg.Client,
		bo:           cfg.Backoff,
		autoComplete: !cfg.NoAutoComplete,
	}, nil
}

// Start initializes the workers and starts polling for new jobs.
func (c *Consumer[P, S, T]) Start(ctx context.Context) (err error) {
	wg := new(sync.WaitGroup)
	ch := make(chan *relay.JobHelper[P, S], c.workers)

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.worker(ctx, ch)
			if err != nil {
				log.Fatal(errors.Wrap(err, "issue encountered processing Jobs"))
			}
		}()
	}

	// avoid the extra goroutine
	if c.pollers == 1 {
		err = c.poller(ctx, ch)
	} else {
		async, ctx := errgroup.WithContext(ctx)
		for i := 0; i < c.pollers; i++ {
			async.Go(func() error {
				return c.poller(ctx, ch)
			})
		}
		err = async.Wait()
	}

	close(ch)
	wg.Wait() // wait for all consumers/processors to finish
	close(c.sem)
	return
}

func (c *Consumer[P, S, T]) poller(ctx context.Context, ch chan<- *relay.JobHelper[P, S]) (err error) {
	var numJobs uint32
	for {
		if numJobs == 0 {
			//fmt.Println("Aquiring blocking")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case c.sem <- struct{}{}:
				numJobs++
			}
		}

		//attempt to maximize acquires into number of Jobs to try and pull.
	FOR:
		for {
			//fmt.Println("Aquiring non-blocking")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case c.sem <- struct{}{}:
				numJobs++
			default:
				break FOR
			}
		}

		var helpers []*relay.JobHelper[P, S]
		//fmt.Println("Fetching:", numJobs)
		helpers, err = c.client.Next(ctx, c.queue, numJobs)
		if err != nil {
			// check for lower level network errors, timeouts, ... and retry automatically
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				continue
			}
			err = errors.Wrap(err, "failed to fetch next Job")
			break
		}
		//fmt.Println("Fetched:", len(helpers))
		for _, jh := range helpers {
			ch <- jh
		}
		if uint32(len(helpers)) > numJobs {
			panic(fmt.Sprintf("helpers: %d num: %d %#v", len(helpers), numJobs, helpers))
		}
		numJobs = uint32(int(numJobs) - len(helpers))
		continue
	}
	return
}

func (c *Consumer[P, S, T]) worker(ctx context.Context, ch <-chan *relay.JobHelper[P, S]) error {
	for helper := range ch {
		if err := c.process(ctx, helper); err != nil {
			return errors.Wrap(err, "processing error")
		}
	}
	return nil
}

func (c *Consumer[P, S, T]) process(ctx context.Context, helper *relay.JobHelper[P, S]) error {
	defer func() {
		//fmt.Println("releasing")
		<-c.sem
		//fmt.Println("released")
	}()

	err := c.processor.Process(ctx, helper)
	if err != nil {
		return errors.Wrap(err, "failed processing Job")
	}

	if c.autoComplete {
		return c.complete(ctx, helper)
	}
	return nil
}

func (c *Consumer[P, S, T]) complete(ctx context.Context, helper *relay.JobHelper[P, S]) (err error) {
	var attempt int
	for {
		if attempt > 0 {
			if err = c.bo.Sleep(ctx, attempt); err != nil {
				// can only happen if context cancelled or timed out
				return err
			}
		}
		err = helper.Complete(ctx)
		if err != nil {
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				attempt++
				continue
			}
			return errors.Wrap(err, "failed to complete Job")
		}
		return nil
	}
}
