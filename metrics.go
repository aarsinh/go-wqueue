package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	jobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_jobs_total",
	}, []string{"result"})

	jobDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "taskqueue_job_duration",
		Buckets: prometheus.DefBuckets,
	})

	queueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "taskqueue_queue_depth",
	}, []string{"queue"})
)
