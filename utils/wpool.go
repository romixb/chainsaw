package utils

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

// BaseJob includes the retry count and common methods for all jobs.
type BaseJob struct {
	retryCount int
	id         int64
}

// IncrementRetryCount increments the retry count for the job.
func (b *BaseJob) IncrementRetryCount() {
	b.retryCount++
}

// RetryCount returns the current retry count for the job.
func (b *BaseJob) RetryCount() int {
	return b.retryCount
}

func (b *BaseJob) ID() int64 {
	return b.id
}

func NewBaseJob(id int64) BaseJob {
	if id == 0 {
		id = atomic.AddInt64(&id, 1)
	}
	return BaseJob{
		id: id,
	}
}

// Job interface represents a unit of work to be processed by a worker.
type Job[T any] interface {
	Process(ctx context.Context) error
	IncrementRetryCount()
	RetryCount() int
	ID() int64
}

// WorkerPool manages a pool of workers to process jobs.
type WorkerPool[T any] struct {
	JobQueue   chan Job[T]
	RetryQueue chan Job[T]
	MaxRetries int
	wg         sync.WaitGroup
}

// NewWorkerPool creates a new WorkerPool.
func NewWorkerPool[T any](maxRetries int) *WorkerPool[T] {
	return &WorkerPool[T]{
		JobQueue:   make(chan Job[T], 1000),
		RetryQueue: make(chan Job[T], 1000),
		MaxRetries: maxRetries,
	}
}

// worker processes jobs from the job and retry queues.
func (wp *WorkerPool[T]) worker(ctx context.Context, workerID int) {
	defer wp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d: Stopping on context cancellation", workerID)
			return
		case job, ok := <-wp.JobQueue:
			if !ok {
				return // JobQueue closed, exit worker
			}
			log.Printf("Worker %d: Processing job %d, current retrycount: %d", workerID, job.ID(), job.RetryCount())
			wp.processJob(ctx, job, workerID)
		case retryJob, ok := <-wp.RetryQueue:
			if !ok {
				return // RetryQueue closed, exit worker
			}
			log.Printf("Worker %d: Processing retry job %d, current retrycount: %d", workerID, retryJob.ID(), retryJob.RetryCount())
			wp.processJob(ctx, retryJob, workerID)
		}
	}
}

// processJob executes the job and sends it to the retry queue if it fails.
func (wp *WorkerPool[T]) processJob(ctx context.Context, job Job[T], workerID int) {
	if err := job.Process(ctx); err != nil {
		log.Printf("Worker %d: Error executing job: %v", workerID, err)
		if IsRetryable(err) && job.RetryCount() < wp.MaxRetries {
			job.IncrementRetryCount()
			select {
			case <-ctx.Done():
				return
			case wp.RetryQueue <- job:
			}
		} else {
			log.Fatalf("Worker %d: Job failed and will not be retried: %v", workerID, err)
		}
	} else {
		log.Printf("Worker %d: Job completed successfully", workerID)
	}
}

// Run starts the worker pool and processes jobs from the job and retry queues.
func (wp *WorkerPool[T]) Run(ctx context.Context, workerCount int) {
	wp.wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go wp.worker(ctx, i)
	}
}

// Wait waits for all workers to finish processing jobs.
func (wp *WorkerPool[T]) Wait() {
	wp.wg.Wait()
}

// RetryableError represents an error that can be retried.
type RetryableError struct {
	msg string
}

// Error implements the error interface.
func (e *RetryableError) Error() string {
	return e.msg
}

// NewRetryableError creates a new RetryableError with a formatted message.
func NewRetryableError(format string, args ...interface{}) *RetryableError {
	return &RetryableError{
		msg: fmt.Sprintf(format, args...),
	}
}

func IsRetryable(err error) bool {
	var retryableErr *RetryableError
	return errors.As(err, &retryableErr)
}
