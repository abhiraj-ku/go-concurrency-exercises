package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

//  Building a production-ready API with Go's concurrency primitives is all about orchestration.
//  You need to spin up workers to do tasks simultaneously (Goroutines),
// 	collect their results safely (Channels), know when they are all finished (WaitGroups),
//  and have an emergency stop button if things take too long or the user disconnects (Context).

// ----------------------------------------	TASK -------------------------------------------------
//  Imagine an endpoint: /weather?city=London. To get the most accurate data,
//  we need to query three different downstream weather services concurrently,
//  aggregate their responses, and return them to the user.
//  If any service takes longer than 2 seconds, we cancel it so the user isn't left hanging.

// Weather result response structure
type WeatherResult struct {
	Provider string  `json:"provider"`
	Temp     float64 `json:"temp,omitempty"`
	Error    string  `json:"error"`
}

func main() {
	http.HandleFunc("/weather", handleGetWeather)
	log.Println("server running on port: 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Handles different providers result concurrently
func handleGetWeather(w http.ResponseWriter, r *http.Request) {
	// extract city from query
	city := r.URL.Query().Get("city")
	if city == "" {
		http.Error(w, "missing required field", http.StatusBadRequest)
		return
	}

	// 1. create context of 2 sec if any provider takes more than that we cancel that
	// default r.Context() will cancel if user cancel the request manually
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	providers := []string{"accuweather", "openWeather", "WeatherAPI"}

	// 2. CREATE a buffered channel to store the results from the providers data
	resultChan := make(chan WeatherResult, len(providers))

	// 3. Waitgroups to track how many goroutines still working
	var wg sync.WaitGroup

	// 4. FAN OUT - one worker gorotunes for each provider call
	for _, p := range providers {
		wg.Add(1)
		go func(provider string) {
			defer wg.Done()
			fetchWeatherData(ctx, provider, city, resultChan)

		}(p)
	}

	// 5. Closer gorutines wait for all groroutines to finish

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 6. FAN IN -> from all grorutines get the result into resulchan and display the result
	var results []WeatherResult
	for res := range resultChan {
		results = append(results, res)
	}

	// send response back
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)

}

// Actual `fetchWeatherData` to simulate the fucntion call
func fetchWeatherData(ctx context.Context, provider string, city string, ch chan<- WeatherResult) {
	// Simulate random network latency between 0.5s and 3s
	latency := time.Duration(rand.Intn(2500)+500) * time.Millisecond

	// Done channel to tell work is done
	workDone := make(chan float64)

	go func() {
		time.Sleep(latency)                 // Simulating the actual HTTP request to the external API
		workDone <- 22.5 + rand.Float64()*5 // Fake temperature
	}()

	// 7. Select statement to pick the case
	select {
	case temp := <-workDone:
		ch <- WeatherResult{Provider: provider, Temp: temp}

	case <-ctx.Done():
		ch <- WeatherResult{Provider: provider, Error: "timeout or canceled"}
	}

}
