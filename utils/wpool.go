package utils

import (
	"context"
	"log"
	"sort"
	"sync"
)

type BaseJob struct {
	retryCount int
	id         int64
}

func (b *BaseJob) IncrementRetryCount() {
	b.retryCount++
}
func (b *BaseJob) RetryCount() int {
	return b.retryCount
}
func (b *BaseJob) ID() int64 {
	return b.id
}
func NewBaseJob(id int64) BaseJob {
	return BaseJob{
		id: id,
	}
}

type Job[T any] interface {
	Process(ctx context.Context) error
	IncrementRetryCount()
	RetryCount() int
	ID() int64
}
type WorkerPool[T any, J Job[T]] interface {
	Run(ctx context.Context, workerCount int)
	Wait()
	SubmitJob(job J)
	ProcessRetries(ctx context.Context)
}
type DefaultWorkerPool[T any, J Job[T]] struct {
	JobQueue   chan J
	MaxRetries int
	wg         WaitGroupWrap
	pwg        WaitGroupWrap
	retryJobs  []J
	mux        sync.Mutex
}

func NewWorkerPool[T any, J Job[T]](queue int, maxRetries int) *DefaultWorkerPool[T, J] {
	return &DefaultWorkerPool[T, J]{
		JobQueue:   make(chan J, queue),
		MaxRetries: maxRetries,
	}
}
func (wp *DefaultWorkerPool[T, J]) worker(ctx context.Context, workerID int) {
	defer wp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d: Stopping due to context cancellation", workerID)
			return
		case job, ok := <-wp.JobQueue:
			if !ok {
				return
			}
			log.Printf("Worker %d: Processing job ID %d", workerID, job.ID())
			wp.processJob(ctx, job, workerID)
		}
	}
}
func (wp *DefaultWorkerPool[T, J]) processJob(ctx context.Context, job J, workerID int) {
	if err := job.Process(ctx); err != nil {
		log.Printf("Worker %d: Error executing job ID %v: %v", workerID, job.ID(), err)
		if IsRetryable(err) && job.RetryCount() <= wp.MaxRetries {
			job.IncrementRetryCount()
			wp.mux.Lock()
			wp.retryJobs = append(wp.retryJobs, job)
			wp.mux.Unlock()
		} else {
			log.Fatalf("Worker %d: Job ID %v failed after %d attempts", workerID, job.ID(), wp.MaxRetries)
		}
	} else {
		log.Printf("Worker %d: Job ID %v completed successfully", workerID, job.ID())
	}
}
func (wp *DefaultWorkerPool[T, J]) Run(ctx context.Context, workerCount int) {
	wp.wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go wp.worker(ctx, i)
	}
}
func (wp *DefaultWorkerPool[T, J]) Wait() {
	wp.wg.Wait()
	//wp.pwg.Wait()
}
func (wp *DefaultWorkerPool[T, J]) ProcessRetries(ctx context.Context) {
	// Sort jobs by ID
	sort.Slice(wp.retryJobs, func(i, j int) bool {
		return wp.retryJobs[i].ID() < wp.retryJobs[j].ID()
	})

	for _, job := range wp.retryJobs {
		wp.processJob(ctx, job, 0) // Worker ID 0 for logging simplicity
	}
}
