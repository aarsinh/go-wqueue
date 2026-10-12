package main

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	keyPending    = "queue:pending"
	keyProcessing = "queue:processing"
	keyDelayed    = "queue:delayed"
	keyDead       = "queue:dead"
	maxRetries    = 5
	doneTTL       = 24 * time.Hour
	deadTTL       = 3 * 24 * time.Hour
)

type Queue struct{ rdb *redis.Client }

func jobKey(id string) string { return "job: " + id }

func (q *Queue) Enqueue(ctx context.Context, id, url string) error {
	pipe := q.rdb.TxPipeline()
	pipe.HSet(ctx, jobKey(id), map[string]any{"url": url, "status": "queued", "attempts": 0})
	pipe.LPush(ctx, keyPending, id)
	_, err := pipe.Exec(ctx)

	return err
}

func (q *Queue) Status(ctx context.Context, id string) (map[string]string, error) {
	return q.rdb.HGetAll(ctx, jobKey(id)).Result()
}

// Moves an element from the right of the pending list to the left of the processing list
// while blocking for 1s. If the element is not in the src list, it blocks until it becomes
// available or 1s passes
func (q *Queue) Next(ctx context.Context) (string, error) {
	return q.rdb.BLMove(ctx, keyPending, keyProcessing, "RIGHT", "LEFT", time.Second).Result()
}

func (q *Queue) Fail(ctx context.Context, id string, cause error) (string, error) {
	jid := jobKey(id)

	attempts, err := q.rdb.HIncrBy(ctx, jid, "attempts", 1).Result()
	if err != nil {
		return "", err
	}

	status := "retrying"
	pipe := q.rdb.TxPipeline()
	pipe.LRem(ctx, keyProcessing, 1, id)
	pipe.HSet(ctx, jid, "error", cause.Error())

	if attempts >= maxRetries {
		status = "dead"
		pipe.LPush(ctx, keyDead, id)
	} else {
		readyWhen := time.Now().Add(backoff(int(attempts))).UnixMilli()
		pipe.ZAdd(ctx, keyDelayed, redis.Z{Score: float64(readyWhen), Member: id})
	}
	pipe.HSet(ctx, jid, "status", status)
	_, err = pipe.Exec(ctx)
	return status, err

}

func (q *Queue) Complete(ctx context.Context, id string) error {
	pipe := q.rdb.TxPipeline()
	pipe.LRem(ctx, keyProcessing, 1, id)
	pipe.HSet(ctx, jobKey(id), "status", "done")
	pipe.Expire(ctx, jobKey(id), doneTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// Requeues jobs left by crashed workers in processing
func (q *Queue) RecoverProcessing(ctx context.Context) error {
	n := 0
	for {
		_, err := q.rdb.LMove(ctx, keyProcessing, keyPending, "RIGHT", "LEFT").Result()
		if err == redis.Nil {
			break
		}
		if err != nil {
			return err
		}
		n++
	}
	slog.Info("recovered in-flight jobs", "count", n)
	return nil
}

// Moves jobs from the delayed queue to the pending queue
func (q *Queue) RunScheduler(ctx context.Context) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		ids, err := q.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     keyDelayed,
			Start:   "-inf",
			Stop:    time.Now().UnixMilli(),
			ByScore: true,
			Count:   100,
		}).Result()

		if err != nil {
			continue
		}
		for _, id := range ids {
			if n, _ := q.rdb.ZRem(ctx, keyDelayed, id).Result(); n == 1 {
				q.rdb.LPush(ctx, keyPending, id)
			}
		}
	}
}

// exponential backoff with equal jitter
func backoff(attempt int) time.Duration {
	d := time.Duration(500*math.Pow(2, float64(attempt-1))) * time.Millisecond
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

func (q *Queue) ReportDepth(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return

		case <-t.C:
		}
		pipe := q.rdb.TxPipeline()
		pending := pipe.LLen(ctx, keyPending)
		processing := pipe.LLen(ctx, keyProcessing)
		delayed := pipe.ZCard(ctx, keyDelayed)
		dead := pipe.LLen(ctx, keyDead)
		if _, err := pipe.Exec(ctx); err == nil {
			queueDepth.WithLabelValues("pending").Set(float64(pending.Val()))
			queueDepth.WithLabelValues("processing").Set(float64(processing.Val()))
			queueDepth.WithLabelValues("delayed").Set(float64(delayed.Val()))
			queueDepth.WithLabelValues("dead").Set(float64(dead.Val()))
		}
	}
}
