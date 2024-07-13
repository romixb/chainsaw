package main

import (
	"context"
	"log"
	"sync"
	"time"
)

type Job[T any] interface {
	Process(ctx context.Context) error
	IncrementRetryCount()
	RetryCount() int
	GetData() T
}

type WorkerPool[T any] struct {
	JobQueue   chan Job[T]
	RetryQueue chan Job[T]
	MaxRetries int
	wg         sync.WaitGroup
}

func NewWorkerPool[T any](queuelen, maxRetries int) *WorkerPool[T] {
	return &WorkerPool[T]{
		JobQueue:   make(chan Job[T], queuelen),
		RetryQueue: make(chan Job[T], queuelen),
		MaxRetries: maxRetries,
	}
}

func (wp *WorkerPool[T]) worker(ctx context.Context, workerID int) {
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
			log.Printf("Worker %d: Processing job", workerID)
			wp.processJob(ctx, job, workerID)
		case retryJob, ok := <-wp.RetryQueue:
			if !ok {
				return
			}
			log.Printf("Worker %d: Processing retry job", workerID)
			wp.processJob(ctx, retryJob, workerID)
		}
	}
}

func (wp *WorkerPool[T]) processJob(ctx context.Context, job Job[T], workerID int) {
	if err := job.Process(ctx); err != nil {
		log.Printf("Worker %d: Error executing job: %v", workerID, err)
		if job.RetryCount() < wp.MaxRetries {
			job.IncrementRetryCount()
			time.Sleep(2 * time.Second)
			select {
			case <-ctx.Done():
				return
			case wp.RetryQueue <- job:
			}
		} else {
			log.Printf("Worker %d: Job failed after %d attempts", workerID, wp.MaxRetries)
		}
	} else {
		log.Printf("Worker %d: Job completed successfully", workerID)
	}
}

func (wp *WorkerPool[T]) Run(ctx context.Context, workerCount int) {
	wp.wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go wp.worker(ctx, i)
	}
}

func (wp *WorkerPool[T]) Wait() {
	wp.wg.Wait()
}
