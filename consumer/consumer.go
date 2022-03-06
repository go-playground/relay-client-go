package consumer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/go-playground/backoff-sys"
	"github.com/go-playground/errors/v5"
	errorsext "github.com/go-playground/pkg/v5/errors"
	"github.com/go-playground/relay-client-go"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Processor represents a processor of Jobs
type Processor interface {
	// Process ...  Warning if returning an error it better be a fatal operational error
	Process(context.Context, *relay.JobHelper) error
}

// Config contains all configuration data to initialize a Consumer for use.
type Config struct {

	// Workers represents the maximum number of Jobs that can be in-flight at one time by having a
	// maximum number of workers.
	// Default will be set to one.
	Workers int

	// Pollers indicates the maximum number of polling workers trying to retrieve Jobs for processing.
	// this should only be tuned for consistent high-flow services for which a single poller becomes the
	// bottleneck which is rare. This should never be set greater than the maximum number of workers and in
	// most cases should be far less than.
	// By default, there will only be one poller which should be all you need 99.99999% of the time.
	Pollers int

	// NoAutoComplete turns off auto-completion of a Job that is processed without error allowing
	// the Processor full control to complete or not as necessary.
	NoAutoComplete bool

	// Processor is the main processor of Jobs.
	Processor Processor

	// Queue is the Jbo Queue for which to pull jobs from for processing.
	Queue string

	// Client represents the pre-configured low-level relay client.
	Client *relay.Client

	// Backoff if the backoff used when calling the `next` or `complete` endpoint and there is no data yet
	// available.
	// Optional: If not set a default backoff is used.
	Backoff backoff.Exponential
}

// Consumer is a wrapper around the low-level Relay Client to abstract away polling and distribution of Jobs
// for processing.
type Consumer struct {
	processor    Processor
	sem          *semaphore.Weighted
	workers      int
	pollers      int
	queue        string
	client       *relay.Client
	bo           backoff.Exponential
	autoComplete bool
}

// New initializes a new Consumer for use.
func New(cfg Config) (*Consumer, error) {
	if cfg.Processor == nil {
		return nil, errors.New("Processor required")
	}
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

	return &Consumer{
		processor:    cfg.Processor,
		sem:          semaphore.NewWeighted(int64(cfg.Workers)),
		workers:      cfg.Workers,
		pollers:      cfg.Pollers,
		queue:        cfg.Queue,
		client:       cfg.Client,
		bo:           cfg.Backoff,
		autoComplete: !cfg.NoAutoComplete,
	}, nil
}

// Start initializes the workers and starts polling for new jobs.
func (c *Consumer) Start(ctx context.Context) (err error) {
	wg := new(sync.WaitGroup)
	ch := make(chan *relay.JobHelper, c.workers)

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
	return
}

func (c *Consumer) poller(ctx context.Context, ch chan<- *relay.JobHelper) (err error) {
	var numJobs uint32
	for {
		//numJobs = 0
		if numJobs == 0 {
			err = c.sem.Acquire(ctx, 1)
			if err != nil {
				break
			}
			numJobs++
		}

		//attempt to maximize acquires into number of Jobs to try and pull.
		for {
			if c.sem.TryAcquire(1) {
				numJobs++
				continue
			}
			break
		}

		// TODO: refactor usage of semaphore to be able to know how many jobs we can automatically query in one go :)
		var helpers []*relay.JobHelper
		helpers, err = c.client.Next(ctx, c.queue, numJobs)
		if err != nil {
			// check for lower level network errors, timeouts, ... and retry automatically
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				c.sem.Release(1)
				continue
			}
			err = errors.Wrap(err, "failed to fetch next Job")
			break
		}
		for _, jh := range helpers {
			ch <- jh
		}
		numJobs = uint32(int(numJobs) - len(helpers))
		continue
	}
	return
}

func (c *Consumer) worker(ctx context.Context, ch <-chan *relay.JobHelper) error {
	for helper := range ch {
		if err := c.process(ctx, helper); err != nil {
			return errors.Wrap(err, "processing error")
		}
	}
	return nil
}

func (c *Consumer) process(ctx context.Context, helper *relay.JobHelper) error {
	defer func() {
		c.sem.Release(1)
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

func (c *Consumer) complete(ctx context.Context, helper *relay.JobHelper) (err error) {
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
