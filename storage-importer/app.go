package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type App struct {
	db         *sql.DB
	httpClient *http.Client
	proxies    *proxyPool
	s3         *s3.Client
	uploader   *manager.Uploader

	bucket     string
	clearFiles bool
	workers    int
	// quality labels ("720", "480") that may be imported; nil means all
	allowedQualities map[string]struct{}
	metrics          *Metrics

	// unix time until which files_storage.m3u8_errors is treated as missing (0 = it exists or is not checked yet)
	m3u8ErrorsMissingUntil atomic.Int64

	jobHeartbeatInterval time.Duration
	jobRecoveryTimeout   time.Duration
	jobRecoveryInterval  time.Duration
}

type processResult struct {
	partialMessage string
}

func compactSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

func formatSQLArgs(args ...any) string {
	if len(args) == 0 {
		return "[]"
	}

	parts := make([]string, 0, len(args))
	for _, arg := range args {
		part := fmt.Sprintf("%v", arg)
		parts = append(parts, truncate(part, 256))
	}

	return "[" + strings.Join(parts, ", ") + "]"
}

func logSQLStart(op, query string, args ...any) time.Time {
	if !debugMySQL {
		return time.Now()
	}
	log.Printf("[mysql] %s start sql=%q args=%s", op, compactSQL(query), formatSQLArgs(args...))
	return time.Now()
}

func logSQLFinish(op string, started time.Time, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		if debugMySQL {
			log.Printf("[mysql] %s empty duration=%s", op, time.Since(started).Round(time.Millisecond))
		}
		return
	}

	if err != nil && mysqlLogErrors {
		log.Printf("[mysql] %s error duration=%s err=%v", op, time.Since(started).Round(time.Millisecond), err)
		return
	}

	if !debugMySQL {
		return
	}

	log.Printf("[mysql] %s ok duration=%s", op, time.Since(started).Round(time.Millisecond))
}

/* -------------------- rewrite + download/upload -------------------- */

// rewriteAndStoreMedia mirrors every resource of a media playlist. A 404 of the source stops it at once and is returned
// as the last value: the resource is gone, so the rest of the file is not worth downloading.
// have holds the names already present in the quality folder of the storage (nil: none); such objects are not downloaded again.
func (a *App) rewriteAndStoreMedia(ctx context.Context, j Job, quality, baseURL string, media Media, have map[string]bool) (Media, int, string, error) {
	// все URI строки: сегменты / init / key и т.д.
	// сделаем:
	// - сегменты нумеруем 1.ts..N.ts
	// - всё остальное оставляем по basename (чтобы не сломать #EXT-X-KEY URI, init.mp4 и т.п.)
	segIndex := 0
	firstFailedURL := ""
	successfulSegments := 0

	totalSeg := 0
	for _, l := range media.Lines {
		if !l.IsTag && looksLikeSegment(l.URI) {
			totalSeg++
		}
	}
	a.metrics.curSegTotal.Set(float64(totalSeg))
	a.metrics.curSegDone.Set(0)
	doneSeg := 0

	rewritten := Media{Lines: make([]MediaLine, 0, len(media.Lines)), HasExtM3U: true}

	for i := 0; i < len(media.Lines); i++ {
		l := media.Lines[i]
		if l.IsTag {
			// Если это #EXT-X-KEY или #EXT-X-MAP и внутри есть URI="..."
			// то тоже нужно скачать/залить и переписать URI на относительный путь.
			if strings.HasPrefix(l.Tag, "#EXT-X-KEY:") || strings.HasPrefix(l.Tag, "#EXT-X-MAP:") {
				newTag, err := a.rewriteURIInsideTag(ctx, j, quality, baseURL, l.Tag, have)
				if err != nil {
					failedURI := resolveURL(baseURL, extractURIFromTag(l.Tag))
					if isSourceNotFound(err) {
						return rewritten, successfulSegments, failedURI, err
					}
					setFirstFailedURL(&firstFailedURL, failedURI)
					log.Printf("[job %d] partial: rewrite tag failed quality=%s url=%s err=%v", j.FileID, quality, failedURI, err)
					rewritten.Lines = append(rewritten.Lines, l)
					continue
				}
				rewritten.Lines = append(rewritten.Lines, MediaLine{IsTag: true, Tag: newTag})
				continue
			}

			rewritten.Lines = append(rewritten.Lines, l)
			continue
		}

		abs := resolveURL(baseURL, l.URI)

		if looksLikeSegment(l.URI) {
			segIndex++
			dstName := fmt.Sprintf("%d.ts", segIndex)
			dstKey := fmt.Sprintf("%s/%s/%s", j.R2Prefix, quality, dstName)

			if have[dstName] {
				// stored by an earlier attempt (an object appears in the storage only when it is complete)
				a.metrics.objectsReused.Inc()
			} else if err := a.mirrorBinary(ctx, abs, dstKey); err != nil {
				if isSourceNotFound(err) {
					return rewritten, successfulSegments, abs, err
				}
				setFirstFailedURL(&firstFailedURL, abs)
				log.Printf("[job %d] partial: segment mirror failed quality=%s url=%s err=%v", j.FileID, quality, abs, err)
				continue
			}
			rewritten.Lines = append(rewritten.Lines, MediaLine{IsTag: false, URI: dstName})
			successfulSegments++
			doneSeg++
			a.metrics.curSegDone.Set(float64(doneSeg))
		} else {
			// init.mp4 / something.bin / subtitles / etc.
			base := path.Base(stripQuery(l.URI))
			if base == "" || base == "." || base == "/" {
				base = "file-" + shortHash(l.URI)
			}
			dstKey := fmt.Sprintf("%s/%s/%s", j.R2Prefix, quality, base)
			if have[base] {
				a.metrics.objectsReused.Inc()
			} else if err := a.mirrorBinary(ctx, abs, dstKey); err != nil {
				if isSourceNotFound(err) {
					return rewritten, successfulSegments, abs, err
				}
				setFirstFailedURL(&firstFailedURL, abs)
				log.Printf("[job %d] partial: asset mirror failed quality=%s url=%s err=%v", j.FileID, quality, abs, err)
				continue
			}
			rewritten.Lines = append(rewritten.Lines, MediaLine{IsTag: false, URI: base})
		}
	}

	return rewritten, successfulSegments, firstFailedURL, nil
}

func (a *App) rewriteURIInsideTag(ctx context.Context, j Job, quality, baseURL, tagLine string, have map[string]bool) (string, error) {
	// ищем URI="..."
	// делаем очень аккуратную замену только этого значения.
	const needle = `URI="`
	pos := strings.Index(tagLine, needle)
	if pos < 0 {
		return tagLine, nil
	}
	start := pos + len(needle)
	end := strings.Index(tagLine[start:], `"`)
	if end < 0 {
		return tagLine, nil
	}
	end = start + end
	origURI := tagLine[start:end]

	abs := resolveURL(baseURL, origURI)

	base := path.Base(stripQuery(origURI))
	if base == "" || base == "." || base == "/" {
		base = "asset-" + shortHash(origURI)
	}
	dstKey := fmt.Sprintf("%s/%s/%s", j.R2Prefix, quality, base)

	if have[base] {
		a.metrics.objectsReused.Inc()
	} else if err := a.mirrorBinary(ctx, abs, dstKey); err != nil {
		return "", err
	}

	newTag := tagLine[:start] + base + tagLine[end:]
	return newTag, nil
}

func (a *App) mirrorBinary(ctx context.Context, srcURL, dstKey string) error {
	// exists, err := a.headObject(ctx, dstKey)
	// if err != nil {
	// 	return err
	// }
	// if exists {
	// 	return nil
	// }

	t0 := time.Now()
	kind := classifyKind(srcURL) // "segment" / "other"

	req, err := http.NewRequestWithContext(ctx, "GET", srcURL, nil)
	if err != nil {
		return err
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.metrics.httpDuration.WithLabelValues(kind).Observe(time.Since(t0).Seconds())
		a.metrics.httpTotal.WithLabelValues(kind, "error").Inc()
		return fmt.Errorf("GET %s: %w", srcURL, err)
	}
	defer resp.Body.Close()

	code := strconv.Itoa(resp.StatusCode)
	a.metrics.httpTotal.WithLabelValues(kind, code).Inc()
	a.metrics.httpDuration.WithLabelValues(kind).Observe(time.Since(t0).Seconds())

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &sourceStatusError{URL: srcURL, Status: resp.StatusCode, Body: sourceBodySnippet(body)}
	}

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}

	var downloaded int64
	body := countingReader{r: resp.Body, n: &downloaded}

	_, err = a.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(dstKey),
		Body:        body,
		ContentType: aws.String(ct),
	})
	if err != nil {
		a.metrics.s3Errors.WithLabelValues("object").Inc()
		return fmt.Errorf("put %s: %w", dstKey, err)
	}

	// байты — после факта
	a.metrics.bytesTotal.WithLabelValues("download").Add(float64(downloaded))
	a.metrics.bytesTotal.WithLabelValues("upload").Add(float64(downloaded))

	// объект по типу
	a.metrics.objectsTotal.WithLabelValues(kind).Inc()

	return nil
}

/* -------------------- DB status helpers -------------------- */

func (a *App) markFailed(ctx context.Context, job Job, err error) error {
	q := `
UPDATE files_storage
SET status='failed', last_error=?, finished_at=NOW()
	WHERE file_id=? AND status='in_progress' AND attempts=?`
	if isPermanentError(err) {
		// a retry would hit the same missing file: use up the attempts so that the job is not claimed again
		q = fmt.Sprintf(`
UPDATE files_storage
SET status='failed', last_error=?, finished_at=NOW(), attempts=GREATEST(attempts, %d)
	WHERE file_id=? AND status='in_progress' AND attempts=?`, maxJobAttempts)
	}
	started := logSQLStart("markFailed", q, truncate(err.Error(), 4000), job.FileID, job.AttemptNumber)
	result, e := a.db.ExecContext(ctx, q, truncate(err.Error(), 4000), job.FileID, job.AttemptNumber)
	logSQLFinish("markFailed", started, e)
	return requireCurrentLease(result, e)
}

func (a *App) markDone(ctx context.Context, job Job, lastError string) error {
	if strings.TrimSpace(lastError) == "" {
		const q = `
UPDATE files_storage
SET status='done', finished_at=NOW(), last_error=NULL
	WHERE file_id=? AND status='in_progress' AND attempts=?`
		started := logSQLStart("markDone", q, job.FileID, job.AttemptNumber)
		result, e := a.db.ExecContext(ctx, q, job.FileID, job.AttemptNumber)
		logSQLFinish("markDone", started, e)
		return requireCurrentLease(result, e)
	}

	const q = `
UPDATE files_storage
SET status='done', finished_at=NOW(), last_error=?
	WHERE file_id=? AND status='in_progress' AND attempts=?`
	started := logSQLStart("markDone", q, truncate(lastError, 4000), job.FileID, job.AttemptNumber)
	result, e := a.db.ExecContext(ctx, q, truncate(lastError, 4000), job.FileID, job.AttemptNumber)
	logSQLFinish("markDone", started, e)
	return requireCurrentLease(result, e)
}

func requireCurrentLease(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return errors.New("job lease is no longer current")
	}
	return nil
}

func (a *App) clearFilesPath(ctx context.Context, fileID uint64) error {
	const q = `UPDATE files SET path='' WHERE id=?`
	started := logSQLStart("clearFilesPath", q, fileID)
	_, e := a.db.ExecContext(ctx, q, fileID)
	logSQLFinish("clearFilesPath", started, e)
	return e
}

// parseQualitiesJSON reads qualities_json (["480/index.m3u8", ...]); anything unreadable counts as empty.
func parseQualitiesJSON(raw string) []string {
	var qs []string
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &qs) != nil {
		return nil
	}
	return qs
}

func qualityURI(quality string) string {
	return quality + "/index.m3u8"
}

// saveQualitiesJSON stores the qualities that are fully imported and are not going to be imported again.
func (a *App) saveQualitiesJSON(ctx context.Context, fileID uint64, qualities []string) error {
	if qualities == nil {
		qualities = []string{}
	}
	val, err := json.Marshal(qualities)
	if err != nil {
		return err
	}
	const q = `UPDATE files_storage SET qualities_json=? WHERE file_id=?`
	started := logSQLStart("saveQualitiesJSON", q, string(val), fileID)
	_, err = a.db.ExecContext(ctx, q, string(val), fileID)
	logSQLFinish("saveQualitiesJSON", started, err)
	return err
}

/* -------------------- fetch helpers -------------------- */

func (a *App) fetchText(ctx context.Context, rawURL string) ([]byte, string, error) {
	t0 := time.Now()
	kind := "m3u8"

	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.metrics.httpDuration.WithLabelValues(kind).Observe(time.Since(t0).Seconds())
		a.metrics.httpTotal.WithLabelValues(kind, "error").Inc()
		return nil, "", err
	}
	defer resp.Body.Close()

	code := strconv.Itoa(resp.StatusCode)
	a.metrics.httpTotal.WithLabelValues(kind, code).Inc()
	a.metrics.httpDuration.WithLabelValues(kind).Observe(time.Since(t0).Seconds())

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, "", &sourceStatusError{URL: rawURL, Status: resp.StatusCode, Body: sourceBodySnippet(body)}
	}

	b, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, "", err
	}
	a.metrics.bytesTotal.WithLabelValues("download").Add(float64(len(b)))
	return b, resp.Request.URL.String(), nil
}

func (a *App) worker(ctx context.Context, idx int) {
	log.Printf("[w%d] worker started", idx)
	lastIdleLog := time.Time{}

	for {
		job, ok, err := a.claimJob(ctx)
		if err != nil {
			log.Printf("[w%d] claim error: %v", idx, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if !ok {
			if time.Since(lastIdleLog) >= 30*time.Second {
				log.Printf("[w%d] idle: no jobs available", idx)
				lastIdleLog = time.Now()
			}
			time.Sleep(1 * time.Second)
			continue
		}
		lastIdleLog = time.Time{}

		log.Printf("[w%d] job file_id=%d src=%s", idx, job.FileID, job.SourceMaster)

		start := time.Now()
		a.metrics.inflightJobs.Inc()

		jobCtx, stopHeartbeat := a.startJobHeartbeat(ctx, job)
		result, processErr := a.processJob(jobCtx, job)
		stopHeartbeat()
		err = processErr
		if err != nil {
			a.metrics.jobsTotal.WithLabelValues("failed").Inc()
			a.metrics.jobDuration.WithLabelValues("failed").Observe(time.Since(start).Seconds())
			log.Printf("[w%d] job file_id=%d FAILED: %v", idx, job.FileID, err)
			if markErr := a.markFailed(ctx, job, err); markErr != nil {
				log.Printf("[w%d] job file_id=%d mark failed skipped: %v", idx, job.FileID, markErr)
			}
		} else {
			a.metrics.lastSuccessTs.Set(float64(time.Now().Unix()))
			outcome := "done"
			if result.partialMessage != "" {
				outcome = "partial"
			}
			a.metrics.jobsTotal.WithLabelValues(outcome).Inc()
			a.metrics.jobDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
			if result.partialMessage != "" {
				log.Printf("[w%d] job file_id=%d DONE PARTIAL: %s", idx, job.FileID, result.partialMessage)
			} else {
				log.Printf("[w%d] job file_id=%d DONE", idx, job.FileID)
			}
			markErr := a.markDone(ctx, job, result.partialMessage)
			if markErr != nil {
				log.Printf("[w%d] job file_id=%d mark done skipped: %v", idx, job.FileID, markErr)
			}
			if markErr == nil && a.clearFiles {
				_ = a.clearFilesPath(ctx, job.FileID)
			}
		}
		a.metrics.inflightJobs.Dec()
	}
}

func (a *App) startJobHeartbeat(parent context.Context, job Job) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(a.jobHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := a.db.ExecContext(ctx, `
UPDATE files_storage
SET started_at=NOW()
WHERE file_id=? AND status='in_progress' AND attempts=?`, job.FileID, job.AttemptNumber)
				if err != nil {
					log.Printf("[heartbeat] file_id=%d attempt=%d error: %v", job.FileID, job.AttemptNumber, err)
					continue
				}
				affected, err := result.RowsAffected()
				if err != nil || affected != 1 {
					log.Printf("[heartbeat] file_id=%d attempt=%d lease lost", job.FileID, job.AttemptNumber)
					cancel()
					return
				}
			}
		}
	}()
	return ctx, func() {
		cancel()
		<-done
	}
}

func (a *App) recoverStaleJobs(ctx context.Context) error {
	result, err := a.db.ExecContext(ctx, `
UPDATE files_storage
SET status='failed', finished_at=NOW(), last_error='worker lease expired'
WHERE status='in_progress'
  AND started_at < DATE_SUB(NOW(), INTERVAL ? SECOND)`, int64(a.jobRecoveryTimeout/time.Second))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		log.Printf("[recovery] returned stale jobs to queue: %d", affected)
	}
	return nil
}

func (a *App) recoveryLoop(ctx context.Context) {
	recover := func() {
		if err := a.recoverStaleJobs(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[recovery] error: %v", err)
		}
	}
	recover()
	ticker := time.NewTicker(a.jobRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recover()
		}
	}
}

func buildPartialMessage(firstFailedURL string) string {
	if strings.TrimSpace(firstFailedURL) == "" {
		return "partial"
	}
	return fmt.Sprintf("partial: first_failed_url=%s", firstFailedURL)
}

func setFirstFailedURL(dst *string, candidate string) {
	if *dst != "" {
		return
	}
	c := strings.TrimSpace(candidate)
	if c == "" {
		return
	}
	*dst = c
}

func extractURIFromTag(tagLine string) string {
	const needle = `URI="`
	pos := strings.Index(tagLine, needle)
	if pos < 0 {
		return ""
	}
	start := pos + len(needle)
	end := strings.Index(tagLine[start:], `"`)
	if end < 0 {
		return ""
	}
	end = start + end
	return tagLine[start:end]
}

func (a *App) claimJob(ctx context.Context) (Job, bool, error) {
	beginStarted := logSQLStart("claimJob.beginTx", "BEGIN")
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{})
	logSQLFinish("claimJob.beginTx", beginStarted, err)
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	var j Job
	var attempts int
	var m3u8Errors int
	var qualitiesJSON sql.NullString
	useErrors := time.Now().Unix() >= a.m3u8ErrorsMissingUntil.Load()
	claimSelect := claimSelectSQL(useErrors)
	selectStarted := logSQLStart("claimJob.select", claimSelect)
	row := tx.QueryRowContext(ctx, claimSelect)
	if useErrors {
		err = row.Scan(&j.FileID, &j.SourceMaster, &j.R2Prefix, &j.R2MasterKey, &attempts, &qualitiesJSON, &m3u8Errors)
	} else {
		err = row.Scan(&j.FileID, &j.SourceMaster, &j.R2Prefix, &j.R2MasterKey, &attempts, &qualitiesJSON)
	}
	logSQLFinish("claimJob.select", selectStarted, err)
	if useErrors && isUnknownColumn(err) {
		// the m3u8_errors migration is not applied yet: claim in the old order and look again later
		a.m3u8ErrorsMissingUntil.Store(time.Now().Add(m3u8ErrorsRecheck).Unix())
		log.Printf("[claim] files_storage.m3u8_errors does not exist yet, jobs are claimed without it for %s", m3u8ErrorsRecheck)
		_ = tx.Rollback()
		return a.claimJob(ctx)
	}
	if errors.Is(err, sql.ErrNoRows) {
		commitStarted := logSQLStart("claimJob.commit-empty", "COMMIT")
		commitErr := tx.Commit()
		logSQLFinish("claimJob.commit-empty", commitStarted, commitErr)
		if commitErr != nil {
			return Job{}, false, commitErr
		}
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	j.AttemptNumber = attempts + 1
	j.Qualities = parseQualitiesJSON(qualitiesJSON.String)

	const claimUpdate = `
UPDATE files_storage
SET status='in_progress', attempts=attempts+1, started_at=NOW(), last_error=NULL
	WHERE file_id=?`
	updateStarted := logSQLStart("claimJob.update", claimUpdate, j.FileID)
	_, err = tx.ExecContext(ctx, claimUpdate, j.FileID)
	logSQLFinish("claimJob.update", updateStarted, err)
	if err != nil {
		return Job{}, false, err
	}

	commitStarted := logSQLStart("claimJob.commit", "COMMIT", j.FileID)
	err = tx.Commit()
	logSQLFinish("claimJob.commit", commitStarted, err)
	if err != nil {
		return Job{}, false, err
	}
	a.metrics.jobsTotal.WithLabelValues("claimed").Inc()
	log.Printf("[claim] file_id=%d attempt=%d m3u8_errors=%d r2_prefix=%s master_key=%s", j.FileID, j.AttemptNumber, m3u8Errors, j.R2Prefix, j.R2MasterKey)
	return j, true, nil
}

func (a *App) processJob(ctx context.Context, j Job) (processResult, error) {
	proxy := a.proxies.next(ctx)
	ctx = withSourceProxy(ctx, proxy)
	a.metrics.proxyJobs.WithLabelValues(proxyEndpoint(proxy)).Inc()
	log.Printf("[job %d] source proxy=%s", j.FileID, proxyEndpoint(proxy))
	log.Printf("[job %d] fetch master %s", j.FileID, j.SourceMaster)
	masterBytes, masterURL, err := a.fetchText(ctx, j.SourceMaster)
	if err != nil {
		if isSourceNotFound(err) {
			return processResult{}, &permanentError{fmt.Errorf("fetch master: %w", err)}
		}
		return processResult{}, fmt.Errorf("fetch master: %w", err)
	}

	master, err := parseM3U8Master(masterBytes)
	// {
	// 	"HeaderLines":["#EXTM3U"],
	// 	"Variants":[
	// 		{
	// 			"StreamInfLine":"#EXT-X-STREAM-INF:RESOLUTION=854x430,BANDWIDTH=714000",
	// 			"URI":"https://ashdi.vip/video9/2/films/morlocks_2011_ukr_hdtvrip_hurtom_143238/hls/480/B66OlH6JjudZmxH+AI8=/index.m3u8",
	// 			"OtherLines":null
	// 		}
	// 	]
	// }
	// logJSON(master)
	// os.Exit(0)
	if err != nil {
		return processResult{}, fmt.Errorf("parse master: %w", err)
	}
	if len(master.Variants) == 0 {
		log.Printf("[job %d] master is single media playlist", j.FileID)
		// Иногда master == media playlist. Тогда считаем это "single quality".
		return a.processSinglePlaylistAsQuality(ctx, j, masterURL, masterBytes)
	}
	log.Printf("[job %d] parsed master variants=%d", j.FileID, len(master.Variants))

	// 1) Сначала обрабатываем каждый variant → сегменты + index.m3u8 в папку качества
	rewrittenVariants := make([]MasterVariant, 0, len(master.Variants))
	playlistsFetched := 0
	playlistsWithChunks := 0
	firstFailedURL := ""

	// qualities imported completely are not imported again, they only go back into the master. qualities_json is just a
	// hint (the bucket may have been replaced), so the storage decides: the playlist of a quality and every chunk it
	// lists must be there. Chunks of a quality that is not complete are reused where they exist.
	complete := make(map[string]bool, len(j.Qualities))
	for _, u := range j.Qualities {
		complete[u] = true
	}
	have := make(map[string]map[string]bool)
	for _, v := range master.Variants {
		quality := guessQualityFromURIOrInf(v.URI, v.StreamInfLine)
		if quality == "" {
			quality = "q" + shortHash(v.URI)
		}
		uri := qualityURI(quality)
		if !complete[uri] && !qualityAllowed(a.allowedQualities, quality, v.StreamInfLine) {
			continue
		}
		ok, names, err := a.verifyQualityInStorage(ctx, j, quality)
		if err != nil {
			// without an answer the qualities_json hint stays; an import would fail on the same storage anyway
			log.Printf("[job %d] cannot verify quality=%s in the storage: %v", j.FileID, quality, err)
			continue
		}
		if complete[uri] && !ok {
			log.Printf("[job %d] quality=%s is in qualities_json but not (completely) in the storage, importing it again", j.FileID, quality)
		}
		if ok {
			log.Printf("[job %d] quality=%s is complete in the storage (%d objects)", j.FileID, quality, len(names))
		}
		complete[uri] = ok
		have[quality] = names
	}
	saved := make([]string, 0, len(complete))
	for _, v := range master.Variants {
		quality := guessQualityFromURIOrInf(v.URI, v.StreamInfLine)
		if quality == "" {
			quality = "q" + shortHash(v.URI)
		}
		if uri := qualityURI(quality); complete[uri] {
			saved = append(saved, uri)
		}
	}
	if !equalStrings(saved, j.Qualities) {
		if err := a.saveQualitiesJSON(ctx, j.FileID, saved); err != nil {
			log.Printf("[job %d] save qualities_json failed: %v", j.FileID, err)
		}
	}
	keptQualities := 0
	attempted := 0
	skippedQualities := []string{}
	for _, v := range master.Variants {
		quality := guessQualityFromURIOrInf(v.URI, v.StreamInfLine)
		if quality == "" {
			quality = "q" + shortHash(v.URI)
		}
		uri := qualityURI(quality)
		if complete[uri] {
			log.Printf("[job %d] quality=%s already imported, skip", j.FileID, quality)
			a.metrics.qualitiesTotal.WithLabelValues("kept").Inc()
			keptQualities++
			v.URI = uri
			rewrittenVariants = append(rewrittenVariants, v)
			continue
		}
		if !qualityAllowed(a.allowedQualities, quality, v.StreamInfLine) {
			skippedQualities = append(skippedQualities, quality)
			a.metrics.qualitiesTotal.WithLabelValues("skipped").Inc()
			continue
		}
		attempted++

		variantAbs := resolveURL(masterURL, v.URI)
		variantBytes, variantURL, err := a.fetchText(ctx, variantAbs)
		if err != nil {
			if isSourceNotFound(err) {
				a.metrics.qualitiesTotal.WithLabelValues("failed").Inc()
				return processResult{}, &permanentError{fmt.Errorf("source file is gone: playlist of quality=%s: %w", quality, err)}
			}
			log.Printf("[job %d] partial: fetch variant failed url=%s err=%v", j.FileID, variantAbs, err)
			setFirstFailedURL(&firstFailedURL, variantAbs)
			a.metrics.qualitiesTotal.WithLabelValues("failed").Inc()
			continue
		}
		playlistsFetched++

		media, err := parseM3U8Media(variantBytes)
		if err != nil {
			log.Printf("[job %d] partial: parse variant failed url=%s err=%v", j.FileID, variantAbs, err)
			setFirstFailedURL(&firstFailedURL, variantAbs)
			a.metrics.qualitiesTotal.WithLabelValues("failed").Inc()
			continue
		}

		log.Printf("[job %d] processing quality=%s variant=%s", j.FileID, quality, variantAbs)

		// качаем/заливаем все “ресурсные URI” плейлиста (сегменты, init.mp4, key и т.п.)
		// и переписываем плейлист на относительные пути с нумерацией ts.
		rewrittenMedia, successfulSegments, mediaFirstFailedURL, goneErr := a.rewriteAndStoreMedia(ctx, j, quality, variantURL, media, have[quality])
		if goneErr != nil {
			a.metrics.qualitiesTotal.WithLabelValues("failed").Inc()
			return processResult{}, &permanentError{fmt.Errorf("source file is gone: quality=%s: %w", quality, goneErr)}
		}
		setFirstFailedURL(&firstFailedURL, mediaFirstFailedURL)
		if successfulSegments == 0 {
			log.Printf("[job %d] partial: no successful chunks for quality=%s variant=%s", j.FileID, quality, variantAbs)
			if mediaFirstFailedURL == "" {
				setFirstFailedURL(&firstFailedURL, variantAbs)
			}
			a.metrics.qualitiesTotal.WithLabelValues("failed").Inc()
			continue
		}

		// upload quality/index.m3u8
		key := fmt.Sprintf("%s/%s/index.m3u8", j.R2Prefix, quality)
		if err := a.putObjectText(ctx, key, rewrittenMedia.Render()); err != nil {
			log.Printf("[job %d] partial: upload quality playlist failed key=%s err=%v", j.FileID, key, err)
			setFirstFailedURL(&firstFailedURL, variantAbs)
			a.metrics.qualitiesTotal.WithLabelValues("failed").Inc()
			continue
		}
		playlistsWithChunks++

		// для master: URI → "<quality>/index.m3u8"
		v.URI = uri
		rewrittenVariants = append(rewrittenVariants, v)

		// only a quality without a single failed chunk counts as imported; the rest is retried on the next run
		if mediaFirstFailedURL != "" {
			a.metrics.qualitiesTotal.WithLabelValues("partial").Inc()
		} else {
			a.metrics.qualitiesTotal.WithLabelValues("imported").Inc()
		}
		if mediaFirstFailedURL == "" {
			complete[uri] = true
			saved = append(saved, uri)
			if err := a.saveQualitiesJSON(ctx, j.FileID, saved); err != nil {
				log.Printf("[job %d] save qualities_json failed: %v", j.FileID, err)
			}
		}
	}

	if len(skippedQualities) > 0 {
		log.Printf("[job %d] skipped qualities not in ALLOWED_QUALITIES: %s", j.FileID, strings.Join(skippedQualities, ","))
	}
	if attempted == 0 && keptQualities == 0 {
		return processResult{}, fmt.Errorf("no variant matches ALLOWED_QUALITIES, source has: %s", strings.Join(skippedQualities, ","))
	}
	if attempted > 0 {
		if playlistsFetched == 0 {
			return processResult{}, fmt.Errorf("no inner playlists fetched")
		}
		if playlistsWithChunks == 0 {
			if firstFailedURL != "" {
				return processResult{}, fmt.Errorf("no chunks imported successfully, first failed url=%s", firstFailedURL)
			}
			return processResult{}, fmt.Errorf("no chunks imported successfully")
		}
	}

	// 2) Собираем master с относительными ссылками, и грузим ПОСЛЕДНИМ
	master.Variants = rewrittenVariants
	masterOut := master.Render()

	// важно: master заливаем последним
	if attempted == 0 && a.storedMasterEquals(ctx, j.R2MasterKey, masterOut) {
		log.Printf("[job %d] everything is already in the storage, nothing to import (master key=%s is up to date)", j.FileID, j.R2MasterKey)
	} else if err := a.putObjectText(ctx, j.R2MasterKey, masterOut); err != nil {
		return processResult{}, fmt.Errorf("upload master %s: %w", j.R2MasterKey, err)
	} else {
		log.Printf("[job %d] uploaded master key=%s", j.FileID, j.R2MasterKey)
	}

	// qualities_json: only fully imported qualities, in master order
	finalQualities := make([]string, 0, len(rewrittenVariants))
	for _, v := range rewrittenVariants {
		if complete[v.URI] {
			finalQualities = append(finalQualities, v.URI)
		}
	}
	if err := a.saveQualitiesJSON(ctx, j.FileID, finalQualities); err != nil {
		log.Printf("[job %d] save qualities_json failed: %v", j.FileID, err)
	}

	if firstFailedURL != "" {
		return processResult{partialMessage: buildPartialMessage(firstFailedURL)}, nil
	}

	return processResult{}, nil
}

func (a *App) processSinglePlaylistAsQuality(ctx context.Context, j Job, baseURL string, mediaBytes []byte) (processResult, error) {
	media, err := parseM3U8Media(mediaBytes)

	if err != nil {
		return processResult{}, fmt.Errorf("parse as media: %w", err)
	}
	quality := "default"
	var have map[string]bool
	if ok, names, err := a.verifyQualityInStorage(ctx, j, quality); err != nil {
		log.Printf("[job %d] cannot verify quality=%s in the storage: %v", j.FileID, quality, err)
	} else if ok {
		log.Printf("[job %d] single playlist is complete in the storage, nothing to import", j.FileID)
		a.metrics.qualitiesTotal.WithLabelValues("kept").Inc()
		if !equalStrings(j.Qualities, []string{qualityURI(quality)}) {
			_ = a.saveQualitiesJSON(ctx, j.FileID, []string{qualityURI(quality)})
		}
		return processResult{}, nil
	} else {
		have = names
	}
	log.Printf("[job %d] processing single playlist quality=%s", j.FileID, quality)

	rewrittenMedia, successfulSegments, firstFailedURL, goneErr := a.rewriteAndStoreMedia(ctx, j, quality, baseURL, media, have)
	if goneErr != nil {
		a.metrics.qualitiesTotal.WithLabelValues("failed").Inc()
		return processResult{}, &permanentError{fmt.Errorf("source file is gone: %w", goneErr)}
	}
	if successfulSegments == 0 {
		if firstFailedURL != "" {
			return processResult{}, fmt.Errorf("no chunks imported successfully, first failed url=%s", firstFailedURL)
		}
		return processResult{}, fmt.Errorf("no chunks imported successfully")
	}

	// upload quality index
	qKey := fmt.Sprintf("%s/%s/index.m3u8", j.R2Prefix, quality)
	if err := a.putObjectText(ctx, qKey, rewrittenMedia.Render()); err != nil {
		return processResult{}, err
	}

	// master, который просто ссылается на default/index.m3u8
	masterOut := "#EXTM3U\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=1\n" +
		quality + "/index.m3u8\n"

	if err := a.putObjectText(ctx, j.R2MasterKey, masterOut); err != nil {
		return processResult{}, err
	}

	if firstFailedURL != "" {
		return processResult{partialMessage: buildPartialMessage(firstFailedURL)}, nil
	}

	if err := a.saveQualitiesJSON(ctx, j.FileID, []string{qualityURI(quality)}); err != nil {
		log.Printf("[job %d] save qualities_json failed: %v", j.FileID, err)
	}
	return processResult{}, nil
}
