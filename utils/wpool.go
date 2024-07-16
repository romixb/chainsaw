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
	JobQueue       chan Job[T]
	RetryQueue     chan Job[T]
	MaxRetries     int
	retryJobIDs    map[int64]struct{}
	wg             WaitGroupWrap
	pwg            WaitGroupWrap
	retryJobIDsMux sync.Mutex
}

// NewWorkerPool creates a new WorkerPool.
func NewWorkerPool[T any](maxRetries int) *WorkerPool[T] {
	return &WorkerPool[T]{
		JobQueue:    make(chan Job[T], 1000), //TODO: parametrise buffer size depending on job amount
		RetryQueue:  make(chan Job[T], 1000),
		MaxRetries:  maxRetries,
		retryJobIDs: make(map[int64]struct{}),
	}
}

// worker processes jobs from the job and retry queues.
func (wp *WorkerPool[T]) worker(ctx context.Context, workerID int) {
	defer wp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d: Stopping due to context cancellation", workerID)
			return
		case job, ok := <-wp.JobQueue:
			if !ok {
				wp.JobQueue = nil
				continue
			}
			log.Printf("Worker %d: Processing job ID %d", workerID, job.ID())
			wp.processJob(ctx, job, workerID)
		case retryJob, ok := <-wp.RetryQueue:
			if !ok {
				wp.RetryQueue = nil
				continue
			}
			wp.retryJobIDsMux.Lock()
			delete(wp.retryJobIDs, retryJob.ID())
			wp.retryJobIDsMux.Unlock()
			log.Printf("Worker %d: Processing retry job ID %d", workerID, retryJob.ID())
			wp.processJob(ctx, retryJob, workerID)
		default:
			// unsafe heuristics due to unknown state of retries
			if wp.JobQueue == nil && wp.pwg.Counter() == 0 && len(wp.RetryQueue) == 0 {
				log.Printf("Worker %d: JobQueue and RetryQueue seem to be empty", workerID)
				return
			}
		}
	}
}

// processJob executes the job and sends it to the retry queue if it fails.
func (wp *WorkerPool[T]) processJob(ctx context.Context, job Job[T], workerID int) {
	wp.pwg.Add(1)
	defer wp.pwg.Done()
	if err := job.Process(ctx); err != nil {
		log.Printf("Worker %d: Error executing job ID %d: %v", workerID, job.ID(), err)
		if IsRetryable(err) && job.RetryCount() < wp.MaxRetries {
			job.IncrementRetryCount()
			wp.retryJobIDsMux.Lock()
			if _, exists := wp.retryJobIDs[job.ID()]; !exists {
				log.Printf("Worker %d: sending job ID %d to retry", workerID, job.ID())
				wp.retryJobIDs[job.ID()] = struct{}{}
				wp.retryJobIDsMux.Unlock() // Ensure the mutex is unlocked before the select statement
				select {
				case <-ctx.Done():
					return
				case wp.RetryQueue <- job:
				}
			} else {
				wp.retryJobIDsMux.Unlock() // Unlock if the job ID already exists
			}
		} else {
			log.Fatalf("Worker %d: Job ID %d failed after %d attempts with err: %v", workerID, job.ID(), wp.MaxRetries, err)
		}
	} else {
		log.Printf("Worker %d: Job ID %d completed successfully", workerID, job.ID())
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
	wp.pwg.Wait()
	wp.wg.Wait()
	close(wp.RetryQueue)
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
