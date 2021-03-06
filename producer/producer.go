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
type Enqueuer[P any, S any] interface {
	Enqueue(context.Context, relay.Job[P, S]) error
	EnqueueBatch(context.Context, []relay.Job[P, S]) error
}

// Config contains all information needed to initialize a Producer.
type Config[P any, S any, T Enqueuer[P, S]] struct {
	// Enqueuer represents the enqueuer of Jobs. This can be the *relay.Client or other wrapper.
	Enqueuer T

	// Backoff if the backoff used when calling `enqueue`.
	// Optional: If not set a default backoff is used.
	Backoff backoff.Exponential
}

// Producer is a wrapper around the low-level Relay Client to abstract away retrying and other bits.
type Producer[P any, S any, T Enqueuer[P, S]] struct {
	enqueuer T
	bo       backoff.Exponential
}

// New create a Producer for use.
func New[P any, S any, T Enqueuer[P, S]](cfg Config[P, S, T]) (*Producer[P, S, T], error) {
	defaultBackoff := backoff.Exponential{}
	if cfg.Backoff == defaultBackoff {
		cfg.Backoff = backoff.NewExponential().Interval(time.Millisecond * 100).Jitter(time.Millisecond * 25).Max(time.Second).Init()
	}

	return &Producer[P, S, T]{
		enqueuer: cfg.Enqueuer,
		bo:       cfg.Backoff,
	}, nil
}

// Enqueue submits the provided Job for processing to the Job Server using the provided Enqueuer.
func (p *Producer[P, S, T]) Enqueue(ctx context.Context, job relay.Job[P, S]) (err error) {
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
func (p *Producer[P, S, T]) EnqueueBatch(ctx context.Context, jobs []relay.Job[P, S]) (err error) {
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
