package main

import (
	"fmt"
	"os"
	"runtime/trace"
	"sync"
	"time"
)

func worker(jobs chan string, wg *sync.WaitGroup, result chan string) {
	defer wg.Done()

	for job := range jobs {
		time.Sleep(50 * time.Millisecond)
		result <- fmt.Sprintf("processed %s", job)
	}

}

func main() {
	jobQue := []string{
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

	traceFile, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer traceFile.Close()

	if err := trace.Start(traceFile); err != nil {
		panic(err)
	}
	defer trace.Stop()

	noOfWorks := 5
	jobs := make(chan string, len(jobQue))
	result := make(chan string, len(jobQue))

	for i := 0; i < noOfWorks; i++ {
		wg.Add(1)
		go worker(jobs, &wg, result)
	}

	for _, job := range jobQue {
		jobs <- job
	}
	close(jobs)

	// in separate go routine because ye block kar dega na
	go func() {
		wg.Wait()
		close(result)
	}()

	for processed := range result {
		fmt.Printf("hey we processed %s\n", processed)
	}

	fmt.Printf("time taken: %v\n", time.Since(start))
}
