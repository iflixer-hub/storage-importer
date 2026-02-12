package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	jobsTotal    *prometheus.CounterVec
	jobDuration  *prometheus.HistogramVec
	inflightJobs prometheus.Gauge

	httpTotal    *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec

	bytesTotal   *prometheus.CounterVec
	objectsTotal *prometheus.CounterVec

	curSegTotal prometheus.Gauge
	curSegDone  prometheus.Gauge

	lastSuccessTs prometheus.Gauge

	dbJobsTotal   prometheus.Gauge
	dbJobsDone    prometheus.Gauge
	dbJobsRunning prometheus.Gauge
	dbJobsFailed  prometheus.Gauge
}

func NewMetrics() *Metrics {
	m := &Metrics{
		jobsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "import_jobs_total", Help: "Total jobs by result"},
			[]string{"result"},
		),
		jobDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "import_job_duration_seconds",
				Help: "Job processing duration",
				// дефолтные бакеты норм, но можно под видео:
				Buckets: []float64{0.5, 1, 2, 5, 10, 20, 40, 80, 160, 320, 600, 1200},
			},
			[]string{"result"},
		),
		inflightJobs: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "import_inflight_jobs", Help: "Jobs currently in progress"},
		),

		httpTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "import_http_requests_total", Help: "HTTP requests count"},
			[]string{"kind", "code"},
		),
		httpDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "import_http_duration_seconds",
				Help:    "HTTP request duration",
				Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 60},
			},
			[]string{"kind"},
		),

		bytesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "import_bytes_total", Help: "Bytes transferred"},
			[]string{"dir"},
		),
		objectsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "import_objects_total", Help: "Objects stored to R2"},
			[]string{"type"},
		),

		curSegTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "import_current_job_segments_total", Help: "Segments total in current job (best-effort)",
		}),
		curSegDone: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "import_current_job_segments_done", Help: "Segments done in current job (best-effort)",
		}),

		lastSuccessTs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "import_last_success_timestamp_seconds",
			Help: "Unix timestamp of last successful job",
		}),
		dbJobsTotal:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "import_db_jobs_total", Help: "Total jobs in DB"}),
		dbJobsDone:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "import_db_jobs_done", Help: "Done jobs in DB"}),
		dbJobsRunning: prometheus.NewGauge(prometheus.GaugeOpts{Name: "import_db_jobs_running", Help: "Running jobs in DB"}),
		dbJobsFailed:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "import_db_jobs_failed", Help: "Failed jobs in DB"}),
	}

	prometheus.MustRegister(
		m.jobsTotal, m.jobDuration, m.inflightJobs,
		m.httpTotal, m.httpDuration,
		m.bytesTotal, m.objectsTotal,
		m.curSegTotal, m.curSegDone,
		m.lastSuccessTs,
		m.dbJobsTotal, m.dbJobsDone, m.dbJobsRunning, m.dbJobsFailed,
	)
	return m
}

func envString(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func serveMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("metrics listening on %s/metrics", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("metrics server error: %v", err)
	}
}
