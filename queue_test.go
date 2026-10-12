package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func TestBackoffBounds(t *testing.T) {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		full := time.Duration(500<<(attempt-1)) * time.Millisecond
		for i := 0; i < 200; i++ {
			d := backoff(attempt)
			if d < full/2 || d > full {
				t.Fatalf("attempt %d: backoff %v outside [%v, %v]", attempt, d, full/2, full)
			}
		}
	}
}

// newTestQueue connects to Redis DB 15 and skips the test when Redis is unreachable.
func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s: %v", addr, err)
	}
	rdb.FlushDB(ctx)
	t.Cleanup(func() {
		rdb.FlushDB(ctx)
		rdb.Close()
	})
	return &Queue{rdb: rdb}
}

func TestEnqueueNextComplete(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "a", "http://example.com"); err != nil {
		t.Fatal(err)
	}
	id, err := q.Next(ctx)
	if err != nil || id != "a" {
		t.Fatalf("Next = %q, %v", id, err)
	}
	if n := q.rdb.LLen(ctx, keyProcessing).Val(); n != 1 {
		t.Fatalf("processing length = %d, want 1", n)
	}
	if err := q.Complete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	st, _ := q.Status(ctx, "a")
	if st["status"] != "done" {
		t.Fatalf("status = %q, want done", st["status"])
	}
	if n := q.rdb.LLen(ctx, keyProcessing).Val(); n != 0 {
		t.Fatalf("processing length = %d, want 0", n)
	}
}

func TestFailRetriesThenDeadLetters(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	q.Enqueue(ctx, "a", "http://example.com")

	for attempt := 1; attempt < maxRetries; attempt++ {
		state, err := q.Fail(ctx, "a", errors.New("boom"))
		if err != nil || state != "retrying" {
			t.Fatalf("attempt %d: state %q, err %v", attempt, state, err)
		}
	}
	if n := q.rdb.ZCard(ctx, keyDelayed).Val(); n != 1 {
		t.Fatalf("delayed size = %d, want 1", n)
	}
	state, err := q.Fail(ctx, "a", errors.New("boom"))
	if err != nil || state != "dead" {
		t.Fatalf("final attempt: state %q, err %v", state, err)
	}
	if n := q.rdb.LLen(ctx, keyDead).Val(); n != 1 {
		t.Fatalf("dead letter size = %d, want 1", n)
	}
	st, _ := q.Status(ctx, "a")
	if st["attempts"] != "5" || st["error"] != "boom" {
		t.Fatalf("job hash = %v", st)
	}
}

func TestRecoverProcessing(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	q.Enqueue(ctx, "a", "u")
	q.Enqueue(ctx, "b", "u")
	q.Next(ctx)
	q.Next(ctx)

	if err := q.RecoverProcessing(ctx); err != nil {
		t.Fatal(err)
	}
	if p, r := q.rdb.LLen(ctx, keyPending).Val(), q.rdb.LLen(ctx, keyProcessing).Val(); p != 2 || r != 0 {
		t.Fatalf("pending %d processing %d, want 2 and 0", p, r)
	}
}

func TestSchedulerMovesDueJobs(t *testing.T) {
	q := newTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	past := float64(time.Now().Add(-time.Second).UnixMilli())
	future := float64(time.Now().Add(time.Hour).UnixMilli())
	q.rdb.ZAdd(ctx, keyDelayed, redis.Z{Score: past, Member: "due"}, redis.Z{Score: future, Member: "later"})

	go q.RunScheduler(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for q.rdb.LLen(ctx, keyPending).Val() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if id := q.rdb.LIndex(ctx, keyPending, 0).Val(); id != "due" {
		t.Fatalf("pending head = %q, want due", id)
	}
	if n := q.rdb.ZCard(ctx, keyDelayed).Val(); n != 1 {
		t.Fatalf("delayed size = %d, want 1", n)
	}
}

func TestHandleSuccessAndFailure(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/bad") {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	q.Enqueue(ctx, "good", srv.URL+"/good")
	q.Enqueue(ctx, "bad", srv.URL+"/bad")
	for i := 0; i < 2; i++ {
		id, err := q.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		handle(q, id)
	}

	if st, _ := q.Status(ctx, "good"); st["status"] != "done" {
		t.Fatalf("good job status = %q", st["status"])
	}
	if st, _ := q.Status(ctx, "bad"); st["status"] != "retrying" {
		t.Fatalf("bad job status = %q", st["status"])
	}
}

func TestReportDepthIncludesDelayed(t *testing.T) {
	q := newTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.rdb.ZAdd(ctx, keyDelayed, redis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: "x"})

	go q.ReportDepth(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rec := httptest.NewRecorder()
		promhttp.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		if strings.Contains(rec.Body.String(), `taskqueue_queue_depth{queue="delayed"} 1`) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("delayed depth never reported as 1")
}
