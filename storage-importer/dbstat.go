package main

import (
	"context"
	"database/sql"
	"time"
)

type DBStats struct {
	Total   float64
	Done    float64
	Running float64
	Failed  float64
}

func (m *Metrics) SetDBStats(s DBStats) {
	m.dbJobsTotal.Set(s.Total)
	m.dbJobsDone.Set(s.Done)
	m.dbJobsRunning.Set(s.Running)
	m.dbJobsFailed.Set(s.Failed)
}

func startDBStatsLoop(ctx context.Context, db *sql.DB, m *Metrics, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	// дернуть сразу, чтобы метрики появились без ожидания
	updateOnce := func() {
		const q = `
SELECT
  COUNT(*) AS total,
  SUM(status='done') AS done,
  SUM(status='in_progress') AS running,
  SUM(status='failed') AS failed
FROM files_storage;`
		var total, done, running, failed sql.NullInt64
		if err := db.QueryRowContext(ctx, q).Scan(&total, &done, &running, &failed); err != nil {
			// не падаем, просто логируем
			// log.Printf("db stats query error: %v", err)
			return
		}
		m.SetDBStats(DBStats{
			Total:   float64(total.Int64),
			Done:    float64(done.Int64),
			Running: float64(running.Int64),
			Failed:  float64(failed.Int64),
		})
	}

	updateOnce()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			updateOnce()
		}
	}
}
