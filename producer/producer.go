package producer

import (
	"context"
	"time"

	"github.com/go-playground/backoff-sys"
	"github.com/go-playground/errors/v5"
	errorsext "github.com/go-playground/pkg/v5/errors"
	"github.com/go-playground/relay-client-go"
)

// Enqueuer represents the enqueuer of Jobs.
type Enqueuer interface {
	Enqueue(context.Context, relay.Job) error
	EnqueueBatch(context.Context, []relay.Job) error
}

// Config contains all information needed to initialize a Producer.
type Config struct {
	// Enqueuer represents the enqueuer of Jobs. This can be the *relay.Client or other wrapper.
	Enqueuer Enqueuer

	// Backoff if the backoff used when calling `enqueue`.
	// Optional: If not set a default backoff is used.
	Backoff backoff.Exponential
}

// Producer is a wrapper around the low-level Relay Client to abstract away retrying and other bits.
type Producer struct {
	enqueuer Enqueuer
	bo       backoff.Exponential
}

// New create a Producer for use.
func New(cfg Config) (*Producer, error) {
	if cfg.Enqueuer == nil {
		return nil, errors.New("Enqueuer is required")
	}
	defaultBackoff := backoff.Exponential{}
	if cfg.Backoff == defaultBackoff {
		cfg.Backoff = backoff.NewExponential().Interval(time.Millisecond * 100).Jitter(time.Millisecond * 25).Max(time.Second).Init()
	}

	return &Producer{
		enqueuer: cfg.Enqueuer,
		bo:       cfg.Backoff,
	}, nil
}

// Enqueue submits the provided Job for processing to the Job Server using the provided Enqueuer.
func (p *Producer) Enqueue(ctx context.Context, job relay.Job) (err error) {
	var attempt int
	for {
		if attempt > 0 {
			if err = p.bo.Sleep(ctx, attempt); err != nil {
				// can only happen if context cancelled or timed out
				return err
			}
		}
		if err = p.enqueuer.Enqueue(ctx, job); err != nil {
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				attempt++
				continue
			}
			return errors.Wrap(err, "failed to enqueue Job")
		}
		return nil
	}
}

// EnqueueBatch submits multiple Jobs for processing to the Job Server in one call using the provided Enqueuer.
func (p *Producer) EnqueueBatch(ctx context.Context, jobs []relay.Job) (err error) {
	var attempt int
	for {
		if attempt > 0 {
			if err = p.bo.Sleep(ctx, attempt); err != nil {
				// can only happen if context cancelled or timed out
				return err
			}
		}
		if err = p.enqueuer.EnqueueBatch(ctx, jobs); err != nil {
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				attempt++
				continue
			}
			return errors.Wrap(err, "failed to enqueue Jobs")
		}
		return nil
	}
}
