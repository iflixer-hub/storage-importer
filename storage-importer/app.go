package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type App struct {
	db         *sql.DB
	httpClient *http.Client
	s3         *s3.Client
	uploader   *manager.Uploader

	bucket     string
	clearFiles bool
	workers    int
	metrics    *Metrics
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

func (a *App) rewriteAndStoreMedia(ctx context.Context, j Job, quality, baseURL string, media Media) (Media, int, string) {
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
				newTag, err := a.rewriteURIInsideTag(ctx, j, quality, baseURL, l.Tag)
				if err != nil {
					failedURI := resolveURL(baseURL, extractURIFromTag(l.Tag))
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

			if err := a.mirrorBinary(ctx, abs, dstKey); err != nil {
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
			if err := a.mirrorBinary(ctx, abs, dstKey); err != nil {
				setFirstFailedURL(&firstFailedURL, abs)
				log.Printf("[job %d] partial: asset mirror failed quality=%s url=%s err=%v", j.FileID, quality, abs, err)
				continue
			}
			rewritten.Lines = append(rewritten.Lines, MediaLine{IsTag: false, URI: base})
		}
	}

	return rewritten, successfulSegments, firstFailedURL
}

func (a *App) rewriteURIInsideTag(ctx context.Context, j Job, quality, baseURL, tagLine string) (string, error) {
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

	if err := a.mirrorBinary(ctx, abs, dstKey); err != nil {
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
		return fmt.Errorf("GET %s status=%d body=%q", srcURL, resp.StatusCode, string(body))
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

func (a *App) markFailed(ctx context.Context, fileID uint64, err error) error {
	const q = `
UPDATE files_storage
SET status='failed', last_error=?, finished_at=NOW()
	WHERE file_id=?`
	started := logSQLStart("markFailed", q, truncate(err.Error(), 4000), fileID)
	_, e := a.db.ExecContext(ctx, q, truncate(err.Error(), 4000), fileID)
	logSQLFinish("markFailed", started, e)
	return e
}

func (a *App) markDone(ctx context.Context, fileID uint64, lastError string) error {
	if strings.TrimSpace(lastError) == "" {
		const q = `
UPDATE files_storage
SET status='done', finished_at=NOW(), last_error=NULL
	WHERE file_id=?`
		started := logSQLStart("markDone", q, fileID)
		_, e := a.db.ExecContext(ctx, q, fileID)
		logSQLFinish("markDone", started, e)
		return e
	}

	const q = `
UPDATE files_storage
SET status='done', finished_at=NOW(), last_error=?
	WHERE file_id=?`
	started := logSQLStart("markDone", q, truncate(lastError, 4000), fileID)
	_, e := a.db.ExecContext(ctx, q, truncate(lastError, 4000), fileID)
	logSQLFinish("markDone", started, e)
	return e
}

func (a *App) clearFilesPath(ctx context.Context, fileID uint64) error {
	const q = `UPDATE files SET path='' WHERE id=?`
	started := logSQLStart("clearFilesPath", q, fileID)
	_, e := a.db.ExecContext(ctx, q, fileID)
	logSQLFinish("clearFilesPath", started, e)
	return e
}

func (a *App) saveQualitiesJSON(ctx context.Context, fileID uint64, m Master) error {
	// минимально: просто список uri из master (уже относительные)
	qs := make([]string, 0, len(m.Variants))
	for _, v := range m.Variants {
		qs = append(qs, v.URI)
	}
	// без JSON энкодера для краткости:
	val := `["` + strings.Join(qs, `","`) + `"]`
	const q = `UPDATE files_storage SET qualities_json=? WHERE file_id=?`
	started := logSQLStart("saveQualitiesJSON", q, val, fileID)
	_, err := a.db.ExecContext(ctx, q, val, fileID)
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
		return nil, "", fmt.Errorf("GET %s status=%d body=%q", rawURL, resp.StatusCode, string(body))
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

		result, processErr := a.processJob(ctx, job)
		err = processErr
		if err != nil {
			a.metrics.jobsTotal.WithLabelValues("failed").Inc()
			a.metrics.jobDuration.WithLabelValues("failed").Observe(time.Since(start).Seconds())
			log.Printf("[w%d] job file_id=%d FAILED: %v", idx, job.FileID, err)
			_ = a.markFailed(ctx, job.FileID, err)
		} else {
			a.metrics.lastSuccessTs.Set(float64(time.Now().Unix()))
			a.metrics.jobsTotal.WithLabelValues("done").Inc()
			a.metrics.jobDuration.WithLabelValues("done").Observe(time.Since(start).Seconds())
			if result.partialMessage != "" {
				log.Printf("[w%d] job file_id=%d DONE PARTIAL: %s", idx, job.FileID, result.partialMessage)
			} else {
				log.Printf("[w%d] job file_id=%d DONE", idx, job.FileID)
			}
			_ = a.markDone(ctx, job.FileID, result.partialMessage)
			if a.clearFiles {
				_ = a.clearFilesPath(ctx, job.FileID)
			}
		}
		a.metrics.inflightJobs.Dec()
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
	const claimSelect = `
SELECT file_id, source_master_url, r2_prefix, r2_master_key, attempts
FROM files_storage
WHERE status IN ('new','failed')
  AND attempts < 10
ORDER BY (status='failed'), attempts, file_id
LIMIT 1
	FOR UPDATE SKIP LOCKED`
	selectStarted := logSQLStart("claimJob.select", claimSelect)
	row := tx.QueryRowContext(ctx, claimSelect)
	err = row.Scan(&j.FileID, &j.SourceMaster, &j.R2Prefix, &j.R2MasterKey, &attempts)
	logSQLFinish("claimJob.select", selectStarted, err)
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
	log.Printf("[claim] file_id=%d attempt=%d r2_prefix=%s master_key=%s", j.FileID, j.AttemptNumber, j.R2Prefix, j.R2MasterKey)
	return j, true, nil
}

func (a *App) processJob(ctx context.Context, j Job) (processResult, error) {
	log.Printf("[job %d] fetch master %s", j.FileID, j.SourceMaster)
	masterBytes, masterURL, err := a.fetchText(ctx, j.SourceMaster)
	if err != nil {
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

	for _, v := range master.Variants {
		variantAbs := resolveURL(masterURL, v.URI)
		variantBytes, variantURL, err := a.fetchText(ctx, variantAbs)
		if err != nil {
			log.Printf("[job %d] partial: fetch variant failed url=%s err=%v", j.FileID, variantAbs, err)
			setFirstFailedURL(&firstFailedURL, variantAbs)
			continue
		}
		playlistsFetched++

		media, err := parseM3U8Media(variantBytes)
		if err != nil {
			log.Printf("[job %d] partial: parse variant failed url=%s err=%v", j.FileID, variantAbs, err)
			setFirstFailedURL(&firstFailedURL, variantAbs)
			continue
		}

		quality := guessQualityFromURIOrInf(v.URI, v.StreamInfLine)
		if quality == "" {
			quality = "q" + shortHash(v.URI)
		}
		log.Printf("[job %d] processing quality=%s variant=%s", j.FileID, quality, variantAbs)

		// качаем/заливаем все “ресурсные URI” плейлиста (сегменты, init.mp4, key и т.п.)
		// и переписываем плейлист на относительные пути с нумерацией ts.
		rewrittenMedia, successfulSegments, mediaFirstFailedURL := a.rewriteAndStoreMedia(ctx, j, quality, variantURL, media)
		setFirstFailedURL(&firstFailedURL, mediaFirstFailedURL)
		if successfulSegments == 0 {
			log.Printf("[job %d] partial: no successful chunks for quality=%s variant=%s", j.FileID, quality, variantAbs)
			if mediaFirstFailedURL == "" {
				setFirstFailedURL(&firstFailedURL, variantAbs)
			}
			continue
		}

		// upload quality/index.m3u8
		key := fmt.Sprintf("%s/%s/index.m3u8", j.R2Prefix, quality)
		if err := a.putObjectText(ctx, key, rewrittenMedia.Render()); err != nil {
			log.Printf("[job %d] partial: upload quality playlist failed key=%s err=%v", j.FileID, key, err)
			setFirstFailedURL(&firstFailedURL, variantAbs)
			continue
		}
		playlistsWithChunks++

		// для master: URI → "<quality>/index.m3u8"
		v.URI = fmt.Sprintf("%s/index.m3u8", quality)
		rewrittenVariants = append(rewrittenVariants, v)
	}

	if playlistsFetched == 0 {
		return processResult{}, fmt.Errorf("no inner playlists fetched")
	}
	if playlistsWithChunks == 0 || len(rewrittenVariants) == 0 {
		if firstFailedURL != "" {
			return processResult{}, fmt.Errorf("no chunks imported successfully, first failed url=%s", firstFailedURL)
		}
		return processResult{}, fmt.Errorf("no chunks imported successfully")
	}

	// 2) Собираем master с относительными ссылками, и грузим ПОСЛЕДНИМ
	master.Variants = rewrittenVariants
	masterOut := master.Render()

	// важно: master заливаем последним
	if err := a.putObjectText(ctx, j.R2MasterKey, masterOut); err != nil {
		return processResult{}, fmt.Errorf("upload master %s: %w", j.R2MasterKey, err)
	}
	log.Printf("[job %d] uploaded master key=%s", j.FileID, j.R2MasterKey)

	// для диагностики можно сохранить qualities_json
	_ = a.saveQualitiesJSON(ctx, j.FileID, master)

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
	log.Printf("[job %d] processing single playlist quality=%s", j.FileID, quality)

	rewrittenMedia, successfulSegments, firstFailedURL := a.rewriteAndStoreMedia(ctx, j, quality, baseURL, media)
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

	return processResult{}, nil
}
