# go-worker-pool

A simple, concurrent worker pool implementation in Go using goroutines, channels, and `context` for graceful shutdown.

## Features

- Fixed number of workers consuming jobs from a buffered channel
- Graceful shutdown on `SIGINT`/`SIGTERM` via `signal.NotifyContext`
- Drains remaining queued jobs on context cancellation before exiting
- Context-aware job submission (blocks safely when the queue is full)

## Usage

```bash
go run main.go
```

## How It Works

1. `NewPool(size, jobsCapacity)` creates a pool with `size` workers and a buffered job queue.
2. `Start(ctx)` spawns the worker goroutines.
3. `Submit(ctx, job)` enqueues a job or returns an error if the context is cancelled.
4. `Shutdown()` closes the job queue and waits for all in-flight/queued jobs to complete.

## License

MIT
