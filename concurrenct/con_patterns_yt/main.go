package main

import (
	"fmt"
	"os"
	"runtime/trace"
	"sync"
	"time"
)

func Bark(done <-chan struct{}, ch chan<- string) {
	defer close(ch)

	for {
		select {
		case <-done:
			return
		case ch <- "Bark":
			time.Sleep(500 * time.Millisecond)
		}
	}
}
func Meow(done <-chan struct{}, ch chan<- string) {
	defer close(ch)
	for {
		select {
		case <-done:
			return
		case ch <- "Meow":
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func main() {

	var wg sync.WaitGroup

	done := make(chan struct{})

	time.AfterFunc(time.Second*1, func() {
		close(done)
	})

	dogs := make(chan string)
	cats := make(chan string)

	traceFile, err := os.Create("trace.out")
	if err != nil {
		fmt.Printf("failed to create trace file: %v", err)
		return
	}

	defer traceFile.Close()

	if err := trace.Start(traceFile); err != nil {
		fmt.Printf("unable to start trace: %v", err)
		return

	}
	defer trace.Stop()

	wg.Add(3)
	go func() {
		defer wg.Done()
		Bark(done, dogs)
	}()
	go func() {
		defer wg.Done()
		Meow(done, cats)
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case val := <-dogs:
				fmt.Println(val)
			case val := <-cats:
				fmt.Println(val)
			}
		}

	}()
	wg.Wait()
}
