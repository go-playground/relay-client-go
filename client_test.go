package relay

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-playground/errors/v5"
	"github.com/stretchr/testify/require"
)

var (
	baseURL    = os.Getenv("RELAY_URL")
	randSource = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func TestOneTimeJob(t *testing.T) {

	assert := require.New(t)
	ctx := context.Background()

	client, err := New[any, any](Config{
		BaseURL: baseURL,
	})
	assert.NoError(err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	id := strconv.Itoa(randSource.Int())
	queue := strconv.Itoa(randSource.Int())
	job := Job[any, any]{
		ID:         id,
		Queue:      queue,
		Timeout:    30,
		MaxRetries: 3,
		RunAt:      &now,
	}

	exists, err := client.ExistsWithRetry(ctx, job.Queue, job.ID)
	assert.NoError(err)
	assert.False(exists)

	err = client.Enqueue(ctx, job)
	assert.NoError(err)

	exists, err = client.ExistsWithRetry(ctx, job.Queue, job.ID)
	assert.NoError(err)
	assert.True(exists)

	jb, err := client.GetWithRetry(ctx, job.Queue, job.ID)
	assert.NoError(err)
	jb.UpdatedAt = nil // set server side by server
	assert.Equal(*jb, job)

	err = client.Enqueue(ctx, job)
	assert.Error(err)
	var e ErrJobExits
	assert.True(errors.As(err, &e))

	jobs, err := client.Next(ctx, job.Queue, 2)
	assert.NoError(err)
	assert.Equal(len(jobs), 1)

	j := jobs[0]

	j.Job().UpdatedAt = nil // set server side by server
	assert.Equal(*j.Job(), job)

	err = j.Heartbeat(ctx, nil)
	assert.NoError(err)

	err = j.CompleteWithRetry(ctx)
	assert.NoError(err)
}

func TestReschedule(t *testing.T) {

	assert := require.New(t)
	ctx := context.Background()

	client, err := New[any, any](Config{
		BaseURL: baseURL,
	})
	assert.NoError(err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	id := strconv.Itoa(randSource.Int())
	queue := strconv.Itoa(randSource.Int())
	job := Job[any, any]{
		ID:         id,
		Queue:      queue,
		Timeout:    30,
		MaxRetries: 3,
		RunAt:      &now,
	}

	exists, err := client.ExistsWithRetry(ctx, job.Queue, job.ID)
	assert.NoError(err)
	assert.False(exists)

	err = client.Enqueue(ctx, job)
	assert.NoError(err)

	exists, err = client.ExistsWithRetry(ctx, job.Queue, job.ID)
	assert.NoError(err)
	assert.True(exists)

	jb, err := client.GetWithRetry(ctx, job.Queue, job.ID)
	assert.NoError(err)
	jb.UpdatedAt = nil // set server side by server
	assert.Equal(*jb, job)

	err = client.Enqueue(ctx, job)
	assert.Error(err)
	var e ErrJobExits
	assert.True(errors.As(err, &e))

	jobs, err := client.Next(ctx, job.Queue, 2)
	assert.NoError(err)
	assert.Equal(len(jobs), 1)

	j := jobs[0]

	j.Job().UpdatedAt = nil // set server side by server
	assert.Equal(*j.Job(), job)

	err = j.Heartbeat(ctx, nil)
	assert.NoError(err)

	err = j.Reschedule(ctx, *j.Job())
	assert.NoError(err)
}

func TestEnqueueMultiple(t *testing.T) {

	assert := require.New(t)
	ctx := context.Background()

	client, err := New[any, any](Config{
		BaseURL: baseURL,
	})
	assert.NoError(err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	id := strconv.Itoa(randSource.Int())
	id2 := strconv.Itoa(randSource.Int())
	queue := strconv.Itoa(randSource.Int())
	job1 := Job[any, any]{
		ID:         id,
		Queue:      queue,
		Timeout:    30,
		MaxRetries: 3,
		RunAt:      &now,
	}
	job2 := Job[any, any]{
		ID:         id2,
		Queue:      queue,
		Timeout:    30,
		MaxRetries: 3,
		RunAt:      &now,
	}

	err = client.EnqueueBatch(ctx, []Job[any, any]{job1, job2})
	assert.NoError(err)

	jobs, err := client.Next(ctx, queue, 3)
	assert.NoError(err)
	assert.Equal(len(jobs), 2)

	j1 := jobs[0]
	j2 := jobs[1]

	j1.Job().UpdatedAt = nil // set server side by server
	j2.Job().UpdatedAt = nil // set server side by server
	assert.Equal(*j1.Job(), job1)
	assert.Equal(*j2.Job(), job2)
}
