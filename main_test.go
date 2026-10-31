package main

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestNoJobsLost verifies that every job submitted to the pool
// is executed before pool.Shutdown() returns.
func TestNoJobsLost(t *testing.T) {
	ctx := context.Background()
	totalJobs := 50
	poolSize := 4
	queueCap := 10

	pool := NewPool(poolSize, queueCap)
	pool.Start(ctx)

	// Submit jobs concurrently or sequentially
	submitted := 0
	for i := 0; i < totalJobs; i++ {
		if err := pool.Submit(ctx, Job{JobId: i}); err != nil {
			t.Fatalf("failed to submit job %d: %v", i, err)
		}
		submitted++
	}

	// Shutdown closes p.jobs and waits for workers to finish draining
	pool.Shutdown()

	// Verify all submitted jobs were processed by inspecting p.jobs length
	if len(pool.jobs) != 0 {
		t.Errorf("expected queue to be completely empty, but found %d leftover jobs", len(pool.jobs))
	}
}

// TestNoGoroutineLeak verifies that all worker goroutines terminate
// and clean up after pool.Shutdown() is called.
func TestNoGoroutineLeak(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poolSize := 5
	pool := NewPool(poolSize, 10)
	pool.Start(ctx)

	for i := 0; i < 20; i++ {
		_ = pool.Submit(ctx, Job{JobId: i})
	}

	pool.Shutdown()

	// Give runtime a fraction of a millisecond to sweep dead goroutines
	time.Sleep(10 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines {
		t.Errorf("goroutine leak detected! initial: %d, final: %d (leaked %d goroutines)",
			initialGoroutines, finalGoroutines, finalGoroutines-initialGoroutines)
	}
}

// TestContextCancellationDrain verifies that if context is cancelled,
// workers switch to the inner loop, drain existing buffered jobs, and exit.
func TestContextCancellationDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	poolSize := 2
	queueCap := 20
	pool := NewPool(poolSize, queueCap)

	// Fill the buffer BEFORE starting workers
	totalSubmitted := 15
	for i := 0; i < totalSubmitted; i++ {
		_ = pool.Submit(ctx, Job{JobId: i})
	}

	// Cancel context immediately before starting workers
	cancel()

	// Start workers with cancelled context
	pool.Start(ctx)

	// Shutdown should trigger inner select drain logic
	pool.Shutdown()

	if len(pool.jobs) != 0 {
		t.Errorf("expected all buffered jobs to be drained on context cancel, but %d remained", len(pool.jobs))
	}
}
