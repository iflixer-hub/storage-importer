package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

type DBStats struct {
	Total   float64
	Done    float64
	Running float64
	Failed  float64
	New     float64
	Partial float64
	Dead    float64
}

func (m *Metrics) SetDBStats(s DBStats) {
	m.dbJobsTotal.Set(s.Total)
	m.dbJobsDone.Set(s.Done)
	m.dbJobsRunning.Set(s.Running)
	m.dbJobsFailed.Set(s.Failed)
	m.dbJobsNew.Set(s.New)
	m.dbJobsPartial.Set(s.Partial)
	m.dbJobsDead.Set(s.Dead)
}

func startDBStatsLoop(ctx context.Context, db *sql.DB, m *Metrics, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	// дернуть сразу, чтобы метрики появились без ожидания
	updateOnce := func() {
		q := fmt.Sprintf(`
SELECT
  COUNT(*) AS total,
  SUM(status='done') AS done,
  SUM(status='in_progress') AS running,
  SUM(status='failed') AS failed,
  SUM(status='new') AS new_jobs,
  SUM(status='done' AND last_error LIKE 'partial%%') AS partial,
  SUM(status='failed' AND attempts >= %d) AS dead
FROM files_storage;`, maxJobAttempts)
		var total, done, running, failed, newJobs, partial, dead sql.NullInt64
		if err := db.QueryRowContext(ctx, q).Scan(&total, &done, &running, &failed, &newJobs, &partial, &dead); err != nil {
			if mysqlLogErrors {
				log.Printf("[mysql] db stats query error: %v", err)
			}
			return
		}
		if debugMySQL {
			log.Printf("[dbstats] total=%d done=%d running=%d failed=%d new=%d partial=%d dead=%d", total.Int64, done.Int64, running.Int64, failed.Int64, newJobs.Int64, partial.Int64, dead.Int64)
		}
		m.SetDBStats(DBStats{
			Total:   float64(total.Int64),
			Done:    float64(done.Int64),
			Running: float64(running.Int64),
			Failed:  float64(failed.Int64),
			New:     float64(newJobs.Int64),
			Partial: float64(partial.Int64),
			Dead:    float64(dead.Int64),
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
