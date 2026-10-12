package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var httpClient = &http.Client{Timeout: 5 * time.Second}

func startWorkers(ctx context.Context, wg *sync.WaitGroup, q *Queue, n int) {
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				id, err := q.Next(ctx)
				if err == redis.Nil {
					continue
				}
				if err != nil {
					if ctx.Err() == nil {
						slog.Error("dequeue failed", "err", err)
						time.Sleep(time.Second)
					}
					continue
				}
				handle(q, id)
			}
		}()
	}
	wg.Add(2)
	go func() { defer wg.Done(); q.RunScheduler(ctx) }()
	go func() { defer wg.Done(); q.ReportDepth(ctx) }()
}

func handle(q *Queue, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	url, err := q.rdb.HGet(ctx, jobKey(id), "url").Result()
	if err == nil {
		err = check(ctx, url)
	}
	jobDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		slog.Warn("job failed", "id", id, "err", err)
		state, ferr := q.Fail(ctx, id, err)
		if ferr != nil {
			slog.Error("fail bookkeeping", "id", id, "err", ferr)
			return
		}
		jobsTotal.WithLabelValues(state).Inc()
		return
	}
	if err := q.Complete(ctx, id); err != nil {
		slog.Error("complete bookkeeping", "id", id, "err", err)
		return
	}
	jobsTotal.WithLabelValues("ok").Inc()
}

func check(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
