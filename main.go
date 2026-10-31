package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Job struct {
	JobId int
}

type Pool struct {
	jobs chan Job
	size int
	wg   sync.WaitGroup
}

func NewPool(size int, jobsCapacity int) *Pool {
	jobs := make(chan Job, jobsCapacity)
	return &Pool{
		jobs: jobs,
		size: size,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.Worker(ctx, i)
	}
}

func (p *Pool) Worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case job, ok := <-p.jobs:
					if !ok {
						return
					}
					fmt.Println("Go routine: ", id, "doing it's job: ", job)
				default:
					fmt.Println("Go routine: ", id, "exiting.")
				}
			}
		case job, ok := <-p.jobs:
			if !ok {
				fmt.Println("Jobs queue closed")
				return
			}
			fmt.Println("Go routine: ", id, "doing it's job: ", job)
		}
	}
}

func (p *Pool) Submit(ctx context.Context, j Job) error {
	select {
	case p.jobs <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) Shutdown() {
	close(p.jobs)
	p.wg.Wait()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := NewPool(5, 10)
	pool.Start(ctx)

	for i := 0; i < 30; i++ {
		if err := pool.Submit(ctx, Job{JobId: i}); err != nil {
			fmt.Println("Error while submitting job: ", err)
			return
		}
	}

	pool.Shutdown() // close queue, wait for in-flight and queued jobs
	fmt.Println("all workers done, exiting cleanly")
}
