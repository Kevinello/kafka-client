package test

import (
	"sync"
	"testing"
	"time"
)

func BenchmarkReset(b *testing.B) {
	testTimer := time.NewTimer(10 * time.Second)
	defer testTimer.Stop()
	var wg sync.WaitGroup
	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			testTimer.Reset(10 * time.Second)
		}()
	}
	wg.Wait()
}

func BenchmarkTimeNow(b *testing.B) {
	timeNow := time.Now()
	timeNow.Day()
	var wg sync.WaitGroup
	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			timeNow = time.Now()
		}()
	}
	wg.Wait()
}
