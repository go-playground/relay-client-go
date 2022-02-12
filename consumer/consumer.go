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
	"golang.org/x/sync/semaphore"
)

// Processor represents a processor of Jobs
type Processor interface {
	// Process ...  Warning if returning an error it better be a fatal operational error
	Process(context.Context, *relay.JobHelper) error
}

// Config contains all configuration data to initialize a Consumer for use.
type Config struct {

	// MaxConcurrentJobs represents the maximum number of Jobs that can be in-flight at one time.
	MaxConcurrentJobs int

	// NoAutoComplete turns off auto-completion of a Job that is processed without error allowing
	// the Processor full control to complete or not as necessary.
	NoAutoComplete bool

	// Processor is the main processor of Jobs.
	Processor Processor

	// Queue is the Jbo Queue for which to pull jobs from for processing.
	Queue string

	// RelayClient represents the pre-configured low-level relay client.
	RelayClient *relay.Client

	// NextBackoff if the backoff used when calling the `next` or `complete` endpoint and there is no data yet
	// available.
	// Optional: If not set a default backoff is used.
	NextBackoff backoff.Exponential
}

// Consumer is a wrapper around the low-level Relay Client to abstract away polling and distribution of Jobs
// for processing.
type Consumer struct {
	processor    Processor
	sem          *semaphore.Weighted
	concurrency  int
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
	if cfg.RelayClient == nil {
		return nil, errors.New("RelayClient required")
	}
	if cfg.MaxConcurrentJobs < 1 {
		return nil, errors.New("MaxConcurrentJobs must be >= 1")
	}

	defaultBackoff := backoff.Exponential{}
	if cfg.NextBackoff == defaultBackoff {
		cfg.NextBackoff = backoff.NewExponential().Interval(time.Millisecond * 100).Jitter(time.Millisecond * 25).Max(time.Second).Init()
	}

	return &Consumer{
		processor:    cfg.Processor,
		sem:          semaphore.NewWeighted(int64(cfg.MaxConcurrentJobs)),
		concurrency:  cfg.MaxConcurrentJobs,
		queue:        cfg.Queue,
		client:       cfg.RelayClient,
		bo:           cfg.NextBackoff,
		autoComplete: !cfg.NoAutoComplete,
	}, nil
}

// Start initializes the workers and starts polling for new jobs.
func (c *Consumer) Start(ctx context.Context) (err error) {
	wg := new(sync.WaitGroup)
	ch := make(chan *relay.JobHelper, c.concurrency)

	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.run(ctx, ch)
			if err != nil {
				log.Fatal(errors.Wrap(err, "issue encountered processing Jobs"))
			}
		}()
	}

	for {
		err = c.sem.Acquire(ctx, 1)
		if err != nil {
			break
		}

		jh, err := c.client.Next(ctx, c.queue)
		if err != nil {
			// check for lower level network errors, timeouts, ... and retry automatically
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				c.sem.Release(1)
				continue
			}
			err = errors.Wrap(err, "failed to initialize Relay Worker")
			break
		}
		ch <- jh
		continue
	}
	close(ch)
	wg.Wait() // wait for all consumers/processors to finish
	return
}

func (c *Consumer) run(ctx context.Context, ch <-chan *relay.JobHelper) error {
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
