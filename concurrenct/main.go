package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/trace"
	"sync"
	"time"
)

type Job struct {
	ID    int
	Image string
}

type Result struct {
	JobID    int
	Image    string
	WorkerID int
	Duration time.Duration
	Err      error
}

func processImage(ctx context.Context, job Job) error {
	processTime := time.Duration(40+(job.ID%5)*20) * time.Millisecond
	timer := time.NewTimer(processTime)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	if job.ID%13 == 0 {
		return errors.New("unsupported image format")
	}

	return nil
}

func worker(ctx context.Context, workerID int, jobs <-chan Job, results chan<- Result, errs chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			started := time.Now()
			err := processImage(ctx, job)
			res := Result{
				JobID:    job.ID,
				Image:    job.Image,
				WorkerID: workerID,
				Duration: time.Since(started),
				Err:      err,
			}

			select {
			case <-ctx.Done():
				return
			case results <- res:
			}

			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				select {
				case errs <- fmt.Errorf("worker %d: job %d failed: %w", workerID, job.ID, err):
				default:
				}
			}
		}
	}
}

func feedJobs(ctx context.Context, queue []string, jobs chan<- Job) {
	defer close(jobs)

	for i, image := range queue {
		job := Job{ID: i + 1, Image: image}
		select {
		case <-ctx.Done():
			return
		case jobs <- job:
		}
	}
}

func main() {
	jobQueue := []string{
		"image1.jpg",
		"image2.jpg",
		"image3.jpg",
		"image4.jpg",
		"image5.jpg",
		"image6.jpg",
		"image7.jpg",
		"image8.jpg",
		"image9.jpg",
		"image10.jpg",
		"image11.jpg",
		"image12.jpg",
		"image13.jpg",
		"image14.jpg",
		"image15.jpg",
		"image16.jpg",
		"image17.jpg",
		"image18.jpg",
		"image19.jpg",
		"image20.jpg",
	}

	var wg sync.WaitGroup
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	traceFile, err := os.Create("trace.out")
	if err != nil {
		fmt.Printf("failed to create trace file: %v\n", err)
		return
	}
	defer traceFile.Close()

	if err := trace.Start(traceFile); err != nil {
		fmt.Printf("failed to start trace: %v\n", err)
		return
	}
	defer trace.Stop()

	workerCount := 5
	jobs := make(chan Job, len(jobQueue))
	results := make(chan Result, len(jobQueue))
	errs := make(chan error, workerCount)

	go feedJobs(ctx, jobQueue, jobs)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(ctx, i+1, jobs, results, errs, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
		close(errs)
	}()

	processedCount := 0
	failureCount := 0

	for results != nil || errs != nil {
		select {
		case <-ctx.Done():
			fmt.Printf("canceled: %v\n", ctx.Err())
			results = nil
			errs = nil
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			fmt.Printf("error: %v\n", err)
		case res, ok := <-results:
			if !ok {
				results = nil
				continue
			}

			processedCount++
			if res.Err != nil {
				failureCount++
				fmt.Printf("worker=%d job=%d image=%s failed after %v: %v\n", res.WorkerID, res.JobID, res.Image, res.Duration, res.Err)
				continue
			}

			fmt.Printf("worker=%d job=%d image=%s done in %v\n", res.WorkerID, res.JobID, res.Image, res.Duration)
		}
	}

	fmt.Printf("processed=%d failed=%d\n", processedCount, failureCount)
	fmt.Printf("time taken: %v\n", time.Since(start))
}
