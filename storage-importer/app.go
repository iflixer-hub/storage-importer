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

/* -------------------- rewrite + download/upload -------------------- */

func (a *App) rewriteAndStoreMedia(ctx context.Context, j Job, quality, baseURL string, media Media) (Media, error) {
	// все URI строки: сегменты / init / key и т.д.
	// сделаем:
	// - сегменты нумеруем 1.ts..N.ts
	// - всё остальное оставляем по basename (чтобы не сломать #EXT-X-KEY URI, init.mp4 и т.п.)
	segIndex := 0

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
					return Media{}, err
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
				return Media{}, err
			}
			rewritten.Lines = append(rewritten.Lines, MediaLine{IsTag: false, URI: dstName})
		} else {
			// init.mp4 / something.bin / subtitles / etc.
			base := path.Base(stripQuery(l.URI))
			if base == "" || base == "." || base == "/" {
				base = "file-" + shortHash(l.URI)
			}
			dstKey := fmt.Sprintf("%s/%s/%s", j.R2Prefix, quality, base)
			if err := a.mirrorBinary(ctx, abs, dstKey); err != nil {
				return Media{}, err
			}
			rewritten.Lines = append(rewritten.Lines, MediaLine{IsTag: false, URI: base})
		}
		doneSeg++
		a.metrics.curSegDone.Set(float64(doneSeg))
	}

	return rewritten, nil
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
	exists, err := a.headObject(ctx, dstKey)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

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
	_, e := a.db.ExecContext(ctx, `
UPDATE files_storage
SET status='failed', last_error=?, finished_at=NOW()
WHERE file_id=?`, truncate(err.Error(), 4000), fileID)
	return e
}

func (a *App) markDone(ctx context.Context, fileID uint64) error {
	_, e := a.db.ExecContext(ctx, `
UPDATE files_storage
SET status='done', finished_at=NOW(), last_error=NULL
WHERE file_id=?`, fileID)
	return e
}

func (a *App) clearFilesPath(ctx context.Context, fileID uint64) error {
	_, e := a.db.ExecContext(ctx, `UPDATE files SET path='' WHERE id=?`, fileID)
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
	_, _ = a.db.ExecContext(ctx, `UPDATE files_storage SET qualities_json=? WHERE file_id=?`, val, fileID)
	return nil
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
	for {
		job, ok, err := a.claimJob(ctx)
		if err != nil {
			log.Printf("[w%d] claim error: %v", idx, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if !ok {
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("[w%d] job file_id=%d src=%s", idx, job.FileID, job.SourceMaster)

		start := time.Now()
		a.metrics.inflightJobs.Inc()

		err = a.processJob(ctx, job)
		if err != nil {
			a.metrics.jobsTotal.WithLabelValues("failed").Inc()
			a.metrics.jobDuration.WithLabelValues("failed").Observe(time.Since(start).Seconds())
			log.Printf("[w%d] job file_id=%d FAILED: %v", idx, job.FileID, err)
			_ = a.markFailed(ctx, job.FileID, err)
		} else {
			a.metrics.jobsTotal.WithLabelValues("done").Inc()
			a.metrics.jobDuration.WithLabelValues("done").Observe(time.Since(start).Seconds())
			log.Printf("[w%d] job file_id=%d DONE", idx, job.FileID)
			_ = a.markDone(ctx, job.FileID)
			if a.clearFiles {
				_ = a.clearFilesPath(ctx, job.FileID)
			}
			a.metrics.lastSuccessTs.Set(float64(time.Now().Unix()))
		}
		a.metrics.inflightJobs.Dec()
	}
}

func (a *App) claimJob(ctx context.Context) (Job, bool, error) {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	var j Job
	var attempts int
	row := tx.QueryRowContext(ctx, `
SELECT file_id, source_master_url, r2_prefix, r2_master_key, attempts
FROM files_storage
WHERE status IN ('new','failed')
  AND attempts < 10
ORDER BY (status='failed'), attempts, file_id
LIMIT 1
FOR UPDATE SKIP LOCKED`)
	err = row.Scan(&j.FileID, &j.SourceMaster, &j.R2Prefix, &j.R2MasterKey, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Commit()
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	j.AttemptNumber = attempts + 1

	_, err = tx.ExecContext(ctx, `
UPDATE files_storage
SET status='in_progress', attempts=attempts+1, started_at=NOW(), last_error=NULL
WHERE file_id=?`, j.FileID)
	if err != nil {
		return Job{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Job{}, false, err
	}
	a.metrics.jobsTotal.WithLabelValues("claimed").Inc()
	return j, true, nil
}

func (a *App) processJob(ctx context.Context, j Job) error {
	masterBytes, masterURL, err := a.fetchText(ctx, j.SourceMaster)
	if err != nil {
		return fmt.Errorf("fetch master: %w", err)
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
		return fmt.Errorf("parse master: %w", err)
	}
	if len(master.Variants) == 0 {
		// Иногда master == media playlist. Тогда считаем это "single quality".
		return a.processSinglePlaylistAsQuality(ctx, j, masterURL, masterBytes)
	}

	// 1) Сначала обрабатываем каждый variant → сегменты + index.m3u8 в папку качества
	rewrittenVariants := make([]MasterVariant, 0, len(master.Variants))

	for _, v := range master.Variants {
		variantAbs := resolveURL(masterURL, v.URI)
		variantBytes, variantURL, err := a.fetchText(ctx, variantAbs)
		if err != nil {
			return fmt.Errorf("fetch variant %s: %w", variantAbs, err)
		}

		media, err := parseM3U8Media(variantBytes)
		if err != nil {
			return fmt.Errorf("parse variant %s: %w", variantAbs, err)
		}

		quality := guessQualityFromURIOrInf(v.URI, v.StreamInfLine)
		if quality == "" {
			quality = "q" + shortHash(v.URI)
		}

		// качаем/заливаем все “ресурсные URI” плейлиста (сегменты, init.mp4, key и т.п.)
		// и переписываем плейлист на относительные пути с нумерацией ts.
		rewrittenMedia, err := a.rewriteAndStoreMedia(ctx, j, quality, variantURL, media)
		if err != nil {
			return fmt.Errorf("store media quality=%s: %w", quality, err)
		}

		// upload quality/index.m3u8
		key := fmt.Sprintf("%s/%s/index.m3u8", j.R2Prefix, quality)
		if err := a.putObjectText(ctx, key, rewrittenMedia.Render()); err != nil {
			return fmt.Errorf("upload %s: %w", key, err)
		}

		// для master: URI → "<quality>/index.m3u8"
		v.URI = fmt.Sprintf("%s/index.m3u8", quality)
		rewrittenVariants = append(rewrittenVariants, v)
	}

	// 2) Собираем master с относительными ссылками, и грузим ПОСЛЕДНИМ
	master.Variants = rewrittenVariants
	masterOut := master.Render()

	// важно: master заливаем последним
	if err := a.putObjectText(ctx, j.R2MasterKey, masterOut); err != nil {
		return fmt.Errorf("upload master %s: %w", j.R2MasterKey, err)
	}

	// для диагностики можно сохранить qualities_json
	_ = a.saveQualitiesJSON(ctx, j.FileID, master)

	return nil
}

func (a *App) processSinglePlaylistAsQuality(ctx context.Context, j Job, baseURL string, mediaBytes []byte) error {
	media, err := parseM3U8Media(mediaBytes)

	if err != nil {
		return fmt.Errorf("parse as media: %w", err)
	}
	quality := "default"

	rewrittenMedia, err := a.rewriteAndStoreMedia(ctx, j, quality, baseURL, media)
	if err != nil {
		return err
	}

	// upload quality index
	qKey := fmt.Sprintf("%s/%s/index.m3u8", j.R2Prefix, quality)
	if err := a.putObjectText(ctx, qKey, rewrittenMedia.Render()); err != nil {
		return err
	}

	// master, который просто ссылается на default/index.m3u8
	masterOut := "#EXTM3U\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=1\n" +
		quality + "/index.m3u8\n"

	if err := a.putObjectText(ctx, j.R2MasterKey, masterOut); err != nil {
		return err
	}

	return nil
}
