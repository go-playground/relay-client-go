package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/backoff-sys"
	"github.com/go-playground/errors/v5"
	errorsext "github.com/go-playground/pkg/v5/errors"
	httpext "github.com/go-playground/pkg/v5/net/http"
	unsafeext "github.com/go-playground/pkg/v5/unsafe"
)

// Job defines all information needed to process a job.
type Job[P any, S any] struct {

	// ID is the unique Job ID which is also CAN be used to ensure the Job is a singleton.
	ID string `json:"id"`

	// Queue is used to differentiate different job types that can be picked up by job runners.
	Queue string `json:"queue"`

	// Timeout denotes the duration, in seconds, after a Job has started processing or since the last
	// heartbeat request occurred before considering the Job failed and being put back into the
	// queue.
	Timeout int32 `json:"timeout"`

	// MaxRetries determines how many times the Job can be retried, due to timeouts, before being considered
	// permanently failed.
	MaxRetries int32 `json:"max_retries,omitempty"`

	// Payload is the raw JSON payload that the job runner will receive.
	Payload P `json:"payload"`

	// State is the raw JSON payload that the job runner will receive.
	//
	// This state will be ignored when enqueueing a Job and can only be set via a Heartbeat
	// request.
	State *S `json:"state,omitempty"`

	// RunAt can optionally schedule/set a Job to be run only at a specific time in the
	// future. This option should mainly be used for one-time jobs and scheduled jobs that have
	// the option of being self-perpetuated in combination with the reschedule endpoint.
	RunAt *time.Time `json:"run_at,omitempty"`

	// UpdatedAt indicates last time the Job was updated either through enqueue, reschedule or heartbeat.
	// This value is for reporting purposes only and will be ignored when enqueuing and rescheduling.
	UpdatedAt *time.Time `json:"updated_at"`
}

// Config contains all information to create a new REaly instance fo use.
type Config struct {
	// BasURL of the HTTP server
	BaseURL string

	// NextBackoff if the backoff used when calling the `next` endpoint and there is no data yet available.
	// Optional: If not set a default backoff is used.
	NextBackoff backoff.Exponential

	// RetryBackoff is the backoff used when calling any of the retryable functions.
	RetryBackoff backoff.Exponential

	// Client is the HTTP Client to use if using a custom one is desired.
	// Optional: If not set it will create a new one cloning the `http.DefaultTransport` and tweaking the settings
	//           for use with sane limits & Defaults.
	Client *http.Client
}

// Client is used to interact with the Client Job Server.
type Client[P any, S any] struct {
	enqueueURL    string
	heartbeatURL  string
	rescheduleURL string
	completeURL   string
	nextURL       string
	nextBo        backoff.Exponential
	retryBo       backoff.Exponential
	client        *http.Client
}

// New creates a new Client instance for use.
func New[P any, S any](cfg Config) (*Client[P, S], error) {
	cfg.BaseURL = strings.TrimSpace(cfg.BaseURL)
	_, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "Invalid URL")
	}
	base := strings.TrimRight(cfg.BaseURL, "/")

	defaultBackoff := backoff.Exponential{}
	if cfg.NextBackoff == defaultBackoff {
		cfg.NextBackoff = backoff.NewExponential().Interval(time.Millisecond * 100).Jitter(time.Millisecond * 25).Max(time.Second).Init()
	}

	if cfg.RetryBackoff == defaultBackoff {
		cfg.RetryBackoff = backoff.NewExponential().Interval(time.Millisecond * 100).Jitter(time.Millisecond * 25).Max(time.Second).Init()
	}

	if cfg.Client == nil {
		trans := http.DefaultTransport.(*http.Transport).Clone()
		trans.MaxConnsPerHost = 1024
		trans.MaxIdleConnsPerHost = 512
		trans.IdleConnTimeout = time.Second * 5

		cfg.Client = &http.Client{
			Transport: trans,
		}
	}

	r := &Client[P, S]{
		enqueueURL:    fmt.Sprintf("%s/v1/jobs", base),
		heartbeatURL:  fmt.Sprintf("%s/v1/jobs/heartbeat", base),
		rescheduleURL: fmt.Sprintf("%s/v1/jobs/reschedule", base),
		completeURL:   fmt.Sprintf("%s/v1/jobs", base),
		nextURL:       fmt.Sprintf("%s/v1/jobs/next", base),
		nextBo:        cfg.NextBackoff,
		retryBo:       cfg.RetryBackoff,
		client:        cfg.Client,
	}
	return r, nil
}

// Enqueue submits the provided Job for processing to the Job Server.
func (r *Client[P, S]) Enqueue(ctx context.Context, job Job[P, S]) error {
	return r.EnqueueBatch(ctx, []Job[P, S]{job})
}

// EnqueueBatch submits one or more Jobs for processing to the Job Server in one call.
func (r *Client[P, S]) EnqueueBatch(ctx context.Context, jobs []Job[P, S]) error {
	b, err := json.Marshal(jobs)
	if err != nil {
		return errors.Wrap(err, "failed to marshal jobs")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.enqueueURL, bytes.NewReader(b))
	if err != nil {
		return errors.Wrap(err, "failed to create enqueue batch request")
	}
	req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)

	resp, err := r.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to make enqueue batch request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusConflict:
		b, _ := io.ReadAll(resp.Body)
		return ErrJobExits{message: unsafeext.BytesToString(b)}
	default:
		if httpext.IsRetryableStatusCode(resp.StatusCode) {
			return retryableErr{err: errors.Newf("Temporary error occurred %d", resp.StatusCode)}
		}
		b, _ := io.ReadAll(resp.Body)
		return errors.Newf("error: %s", unsafeext.BytesToString(b))
	}
}

// Next attempts to retrieve the next Job in the `queue` requested. It will retry and backoff attempting to retrieve
// a Job and will block until retrieving a Job or the Context is cancelled.
func (r *Client[P, S]) Next(ctx context.Context, queue string, num_jobs uint32) ([]*JobHelper[P, S], error) {
	nextURL := r.nextURL + "?queue=" + url.QueryEscape(queue) + "&num_jobs=" + url.QueryEscape(strconv.Itoa(int(num_jobs)))
	var attempt int
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURL, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create next request")
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, errors.Wrap(err, "failed to make next request")
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			var jobs []*Job[P, S]
			err := json.NewDecoder(resp.Body).Decode(&jobs)
			if err != nil {
				// connection must have been disrupted, continue to retrieve, the Job IF lost will
				// be retried.
				continue
			}

			helpers := make([]*JobHelper[P, S], 0, len(jobs))

			for _, j := range jobs {
				j := j
				helpers = append(helpers, &JobHelper[P, S]{
					client:     r,
					httpClient: r.client,
					job:        j,
				})
			}
			return helpers, nil
		default:

			if resp.StatusCode == http.StatusNoContent || httpext.IsRetryableStatusCode(resp.StatusCode) {
				// includes http.StatusNoContent and http.TooManyRequests
				// no new jobs to process
				if err := r.nextBo.Sleep(ctx, attempt); err != nil {
					// only context.Cancel as error ever
					return nil, err
				}
				attempt++
				continue
			}

			return nil, errors.Newf("invalid request, status code: %d", resp.StatusCode)
		}
	}
}

// Remove removes the Job from the DB for processing. In fact this function makes a call to the complete endpoint.
//
// NOTE: It does not matter if the Job is in-flight or not it will be removed. All relevant code paths return an
//
//	ErrNotFound to handle such events within Job Workers so that they can bail gracefully if desired.
func (r *Client[P, S]) Remove(ctx context.Context, queue, jobID string) error {
	values := make(url.Values)
	values.Set("id", jobID)
	values.Set("queue", queue)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.completeURL+"?"+values.Encode(), nil)
	if err != nil {
		return errors.Wrap(err, "failed to create complete request")
	}

	resp, err := r.client.Do(req)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return retryableErr{err: errors.New("Temporary error occurred EOF")}
		}
		return errors.Wrap(err, "failed to make complete request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		b, _ := io.ReadAll(resp.Body)
		return ErrNotFound{message: unsafeext.BytesToString(b)}
	default:
		if httpext.IsRetryableStatusCode(resp.StatusCode) {
			return retryableErr{err: errors.Newf("Temporary error occurred %d", resp.StatusCode)}
		}
		b, _ := io.ReadAll(resp.Body)
		return errors.Newf("error: %s", unsafeext.BytesToString(b))
	}
}

func (r *Client[P, S]) removeRetryable(ctx context.Context, queue, jobID string) error {
	var attempts int
	for {
		if attempts > 0 {
			if err := r.retryBo.Sleep(ctx, attempts); err != nil {
				// can only occur if context cancelled or timout
				return err
			}
		}
		err := r.Remove(ctx, queue, jobID)
		if err != nil {
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				attempts++
				continue
			}
			return errors.Wrap(err, "failed to remove job")
		}
		return nil
	}
}

// JobHelper is used to process an individual Job retrieved from the Job Server. It contains a number of helper methods
// to `Heartbeat` and `Complete` Jobs.
type JobHelper[P any, S any] struct {
	client     *Client[P, S]
	httpClient *http.Client
	cancel     context.CancelFunc
	job        *Job[P, S]
	wg         sync.WaitGroup
}

// Job returns the Job to process
func (j *JobHelper[P, S]) Job() *Job[P, S] {
	return j.job
}

// HeartbeatAuto automatically calls the Job Runners heartbeat endpoint in a separate goroutine on the
// provided interval. It is convenience to use this when no state needs to be saved but Job kept alive.
func (j *JobHelper[P, S]) HeartbeatAuto(ctx context.Context, interval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	j.cancel = cancel
	j.wg.Add(1)
	go func() {
		defer j.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := j.Heartbeat(ctx, nil)
				if err != nil && errors.Is(err, context.Canceled) {
					return
				}
			}
		}
	}()
}

// Heartbeat calls the Job Runners heartbeat endpoint to keep the job alive.
// Optional: It optionally accepts a state payload if desired to be used in case of failure for
//
//	point-in-time restarting.
func (j *JobHelper[P, S]) Heartbeat(ctx context.Context, state *S) error {

	var err error
	var req *http.Request

	values := make(url.Values)
	values.Set("queue", j.Job().Queue)
	values.Set("id", j.Job().ID)

	url := j.client.heartbeatURL + "?" + values.Encode()

	if state != nil {
		var b []byte
		b, err = json.Marshal(state)
		if err != nil {
			return errors.Wrap(err, "failed to marshal heartbeat state")
		}

		req, err = http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(b))
	} else {
		req, err = http.NewRequestWithContext(ctx, http.MethodPatch, url, nil)
	}
	if err != nil {
		return errors.Wrap(err, "failed to create heartbeat request")
	}
	req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)

	resp, err := j.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to make heartbeat request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusNotFound:
		b, _ := io.ReadAll(resp.Body)
		return ErrNotFound{message: unsafeext.BytesToString(b)}
	default:
		if httpext.IsRetryableStatusCode(resp.StatusCode) {
			return retryableErr{err: errors.Newf("Temporary error occurred %d", resp.StatusCode)}
		}
		b, _ := io.ReadAll(resp.Body)
		return errors.Newf("error: %s", unsafeext.BytesToString(b))
	}
}

// Reschedule submits the provided Job for processing by rescheduling an existing Job for another iteration.
func (j *JobHelper[P, S]) Reschedule(ctx context.Context, job Job[P, S]) error {
	b, err := json.Marshal(job)
	if err != nil {
		return errors.Wrap(err, "failed to marshal job")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.client.rescheduleURL, bytes.NewReader(b))
	if err != nil {
		return errors.Wrap(err, "failed to create reschedule request")
	}
	req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)

	resp, err := j.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to make reschedule request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusNotFound:
		b, _ := io.ReadAll(resp.Body)
		return ErrNotFound{message: unsafeext.BytesToString(b)}
	default:
		if httpext.IsRetryableStatusCode(resp.StatusCode) {
			return retryableErr{err: errors.Newf("Temporary error occurred %d", resp.StatusCode)}
		}
		b, _ := io.ReadAll(resp.Body)
		return errors.Newf("error: %s", unsafeext.BytesToString(b))
	}
}

// RescheduleWithRetry is the same as Reschedule but automatically retries on transient errors.
func (j *JobHelper[P, S]) RescheduleWithRetry(ctx context.Context, job Job[P, S]) error {
	var attempts int
	for {
		if attempts > 0 {
			if err := j.client.retryBo.Sleep(ctx, attempts); err != nil {
				// can only occur if context cancelled or timout
				return err
			}
		}
		err := j.Reschedule(ctx, job)
		if err != nil {
			if _, isRetryable := errorsext.IsRetryableHTTP(err); isRetryable {
				attempts++
				continue
			}
			return errors.Wrap(err, "failed to reschedule job")
		}
		return nil
	}
}

// Complete marks the Job as complete. It does NOT matter to the Job Runner if the job was successful or not.
func (j *JobHelper[P, S]) Complete(ctx context.Context) error {
	if j.cancel != nil {
		j.cancel()
		j.wg.Wait()
	}
	err := j.client.Remove(ctx, j.Job().Queue, j.Job().ID)
	if err != nil {
		return errors.Wrap(err, "failed to complete job")
	}
	return nil
}

// CompleteWithRetry is the same as Complete but also automatically retries on transient errors.
func (j *JobHelper[P, S]) CompleteWithRetry(ctx context.Context) error {
	if j.cancel != nil {
		j.cancel()
		j.wg.Wait()
	}
	err := j.client.removeRetryable(ctx, j.Job().Queue, j.Job().ID)
	if err != nil {
		return errors.Wrap(err, "failed to complete job")
	}
	return nil
}

// denotes a retryable error by implementing the `IsTemporary` function.
type retryableErr struct {
	err error
}

// Error returns the error in string form.
func (r retryableErr) Error() string {
	return r.err.Error()
}

// Temporary denotes if this error is retryable.
func (r retryableErr) Temporary() bool {
	return true
}

// ErrNotFound indicates that the queue and/or Job you specified could not be found on the Job Server.
type ErrNotFound struct {
	message string
}

// Error returns the error in string form.
func (e ErrNotFound) Error() string {
	return e.message
}

// ErrJobExits denotes that the Job that was attempted to be submitted/enqueued on the Job Server already exists and
// the Job was not accepted because of this.
type ErrJobExits struct {
	message string
}

// Error returns the error in string form.
func (e ErrJobExits) Error() string {
	return e.message
}
