package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/backoff-sys"
	"github.com/go-playground/errors/v5"
	httpext "github.com/go-playground/pkg/v5/net/http"
	unsafeext "github.com/go-playground/pkg/v5/unsafe"
)

// Job defines all information needed to process a job.
type Job struct {

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
	Payload json.RawMessage `json:"payload"`

	// State is the raw JSON payload that the job runner will receive.
	//
	// This state will be ignored when enqueueing a Job and can only be set via a Heartbeat
	// request.
	State json.RawMessage `json:"state,omitempty"`
}

// Config contains all information to create a new REaly instance fo use.
type Config struct {
	// BasURL of the HTTP server
	BaseURL string

	// NextBackoff if the backoff used when calling the `next` endpoint and there is no data yet available.
	// Optional: If not set a default backoff is used.
	NextBackoff backoff.Exponential

	// Client is the HTTP Client to use if using a custom one is desired.
	// Optional: If not set it will create a new one cloning the `http.DefaultTransport` and tweaking the settings
	//           for use with sane limits & Defaults.
	Client *http.Client
}

// Client is used to interact with the Client Job Server.
type Client struct {
	enqueueURL   string
	heartbeatURL string
	completeURL  string
	nextURL      string
	bo           backoff.Exponential
	client       *http.Client
}

// New creates a new Client instance for use.
func New(cfg Config) (*Client, error) {
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

	if cfg.Client == nil {
		trans := http.DefaultTransport.(*http.Transport).Clone()
		trans.MaxConnsPerHost = 1024
		trans.MaxIdleConnsPerHost = 512
		trans.IdleConnTimeout = time.Second * 5

		cfg.Client = &http.Client{
			Transport: trans,
		}
	}

	r := &Client{
		enqueueURL:   fmt.Sprintf("%s/enqueue", base),
		heartbeatURL: fmt.Sprintf("%s/heartbeat", base),
		completeURL:  fmt.Sprintf("%s/complete", base),
		nextURL:      fmt.Sprintf("%s/next", base),
		bo:           cfg.NextBackoff,
		client:       cfg.Client,
	}
	return r, nil
}

// Enqueue submits the provided Job for processing to the Job Server.
func (r *Client) Enqueue(ctx context.Context, job Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return errors.Wrap(err, "failed to marshal job")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.enqueueURL, bytes.NewReader(b))
	if err != nil {
		return errors.Wrap(err, "failed to create heartbeat request")
	}
	req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)

	resp, err := r.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to make heartbeat request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusConflict:
		b, _ := ioutil.ReadAll(resp.Body)
		return ErrJobExits{message: unsafeext.BytesToString(b)}
	default:
		if httpext.IsRetryableStatusCode(resp.StatusCode) {
			return retryableErr{err: errors.Newf("Temporary error occurred %d", resp.StatusCode)}
		}
		b, _ := ioutil.ReadAll(resp.Body)
		return errors.Newf("error: %s", unsafeext.BytesToString(b))
	}
}

// Next attempts to retrieve the next Job in the `queue` requested. It will retry and backoff attempting to retrieve
// a Job and will block until retrieving a Job or the Context is cancelled.
func (r *Client) Next(ctx context.Context, queue string) (*JobHelper, error) {
	nextURL := r.nextURL + "?queue=" + url.QueryEscape(queue)
	var attempt int
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURL, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create heartbeat request")
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, errors.Wrap(err, "failed to make heartbeat request")
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			var j *Job
			err := json.NewDecoder(resp.Body).Decode(&j)
			if err != nil {
				// connection must have been disrupted, continue to retrieve, the Job IF lost will
				// be retried.
				continue
			}
			values := make(url.Values)
			values.Set("job_id", j.ID)
			values.Set("queue", j.Queue)

			encoded := values.Encode()

			return &JobHelper{
				heartbeatURL: r.heartbeatURL + "?" + encoded,
				completeURL:  r.completeURL + "?" + encoded,
				client:       r.client,
				job:          j,
			}, nil
		default:
			// includes http.StatusNoContent and http.TooManyRequests
			// no new jobs to process
			dur := r.bo.Duration(attempt)
			fmt.Println(attempt, dur)
			if dur < time.Nanosecond {
				panic("WHAT!")
			}
			if err := r.bo.Sleep(ctx, attempt); err != nil {
				// only context.Cancel as error ever
				return nil, err
			}
			attempt++
			continue
		}
	}
}

// JobHelper is used to process an individual Job retrieved from the Job Server. It contains a number of helper methods
// to `Heartbeat` and `Complete` Jobs.
type JobHelper struct {
	heartbeatURL string
	completeURL  string
	client       *http.Client
	cancel       context.CancelFunc
	job          *Job
	wg           sync.WaitGroup
}

// Job returns the Job to process
func (j *JobHelper) Job() *Job {
	return j.job
}

// HeartbeatAuto automatically calls the Job Runners heartbeat endpoint in a separate goroutine on the
// provided interval. It is convenience to use this when no state needs to be saved but Job kept alive.
func (j *JobHelper) HeartbeatAuto(ctx context.Context, interval time.Duration) {
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
//           point-in-time restarting.
func (j *JobHelper) Heartbeat(ctx context.Context, state json.RawMessage) error {

	var err error
	var req *http.Request

	if len(state) > 0 {
		req, err = http.NewRequestWithContext(ctx, http.MethodPatch, j.heartbeatURL, bytes.NewReader(state))
	} else {
		req, err = http.NewRequestWithContext(ctx, http.MethodPatch, j.heartbeatURL, nil)
	}

	if err != nil {
		return errors.Wrap(err, "failed to create heartbeat request")
	}
	req.Header.Set(httpext.ContentType, httpext.ApplicationJSON)

	resp, err := j.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to make heartbeat request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		return nil
	case http.StatusNotFound:
		b, _ := ioutil.ReadAll(resp.Body)
		return ErrNotFound{message: string(b)}
	default:
		if httpext.IsRetryableStatusCode(resp.StatusCode) {
			return retryableErr{err: errors.Newf("Temporary error occurred %d", resp.StatusCode)}
		}
		b, _ := ioutil.ReadAll(resp.Body)
		return errors.Newf("error: %s", string(b))
	}
}

// Complete marks the Job as complete. It does NOT matter to the Job Runner if the job was successful or not.
func (j *JobHelper) Complete(ctx context.Context) error {
	if j.cancel != nil {
		j.cancel()
		j.wg.Wait()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, j.completeURL, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create complete request")
	}

	resp, err := j.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to make complete request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		b, _ := ioutil.ReadAll(resp.Body)
		return ErrNotFound{message: string(b)}
	default:
		if httpext.IsRetryableStatusCode(resp.StatusCode) {
			return retryableErr{err: errors.Newf("Temporary error occurred %d", resp.StatusCode)}
		}
		b, _ := ioutil.ReadAll(resp.Body)
		return errors.Newf("error: %s", string(b))
	}
}

// denotes a retryable error by implementing the `IsTemporary` function.
type retryableErr struct {
	err error
}

// Error returns the error in string form.
func (r retryableErr) Error() string {
	return r.err.Error()
}

// IsTemporary denotes if this error is retryable.
func (r retryableErr) IsTemporary() bool {
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
