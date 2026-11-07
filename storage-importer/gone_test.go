package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// a database that only records the statements it is given
type recordedExec struct {
	query string
	args  []driver.Value
}

type recordingDriver struct {
	mu    sync.Mutex
	execs []recordedExec
}

type recordingConn struct{ d *recordingDriver }
type recordingStmt struct {
	d *recordingDriver
	q string
}

func (d *recordingDriver) Open(string) (driver.Conn, error)   { return recordingConn{d}, nil }
func (c recordingConn) Prepare(q string) (driver.Stmt, error) { return recordingStmt{c.d, q}, nil }
func (c recordingConn) Close() error                          { return nil }
func (c recordingConn) Begin() (driver.Tx, error)             { return nil, nil }
func (s recordingStmt) Close() error                          { return nil }
func (s recordingStmt) NumInput() int                         { return -1 }
func (s recordingStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, io.EOF
}
func (s recordingStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.d.mu.Lock()
	s.d.execs = append(s.d.execs, recordedExec{s.q, args})
	s.d.mu.Unlock()
	return driver.RowsAffected(1), nil
}

var recordingSeq int

type goneFixture struct {
	app    *App
	rec    *recordingDriver
	hits   *sync.Map // paths requested from the source
	srcURL string
	stop   func()
}

func newGoneFixture(t *testing.T, missing func(path string) bool) *goneFixture {
	t.Helper()
	hits := &sync.Map{}
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Store(r.URL.Path, true)
		switch {
		case strings.HasSuffix(r.URL.Path, "master.m3u8"):
			io.WriteString(w, "#EXTM3U\n#EXT-X-STREAM-INF:RESOLUTION=1280x720,BANDWIDTH=2\nhls/720/i.m3u8\n#EXT-X-STREAM-INF:RESOLUTION=854x480,BANDWIDTH=1\nhls/480/i.m3u8\n")
		case strings.HasSuffix(r.URL.Path, "i.m3u8"):
			var b strings.Builder
			b.WriteString("#EXTM3U\n#EXT-X-TARGETDURATION:4\n")
			for i := 1; i <= 5; i++ {
				b.WriteString("#EXTINF:4,\nsegment" + strconv.Itoa(i) + ".ts\n")
			}
			b.WriteString("#EXT-X-ENDLIST\n")
			io.WriteString(w, b.String())
		case missing(r.URL.Path):
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, "<html><body><h1>404 Not Found</h1>"+strings.Repeat("<!-- a padding -->\r\n", 40)+"</body></html>")
		default:
			io.WriteString(w, "TS")
		}
	}))
	s3srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"x"`)
	}))

	rec := &recordingDriver{}
	recordingSeq++
	name := "recording" + strconv.Itoa(recordingSeq)
	sql.Register(name, rec)
	db, _ := sql.Open(name, "")

	cfg, _ := config.LoadDefaultConfig(context.Background(), config.WithRegion("r"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("a", "b", "")))
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(s3srv.URL); o.UsePathStyle = true })
	app := &App{db: db, proxies: &proxyPool{db: db, ttl: time.Hour, loadedAt: time.Now()}, s3: s3c, uploader: manager.NewUploader(s3c),
		bucket: "b", metrics: sharedTestMetrics(), httpClient: src.Client()}
	return &goneFixture{app: app, rec: rec, hits: hits, srcURL: src.URL, stop: func() { src.Close(); s3srv.Close() }}
}

var testMetrics *Metrics

func sharedTestMetrics() *Metrics {
	if testMetrics == nil {
		testMetrics = NewMetrics()
	}
	return testMetrics
}

func (f *goneFixture) hit(path string) bool { _, ok := f.hits.Load(path); return ok }

func TestSegment404FailsTheWholeFileAtOnce(t *testing.T) {
	f := newGoneFixture(t, func(p string) bool { return strings.HasSuffix(p, "/720/segment2.ts") })
	defer f.stop()

	_, err := f.app.processJob(context.Background(), Job{FileID: 7, SourceMaster: f.srcURL + "/m/master.m3u8", R2Prefix: "files/7", R2MasterKey: "files/7/index.m3u8"})
	if err == nil || !isPermanentError(err) || !isSourceNotFound(err) {
		t.Fatalf("expected a permanent 404 error, got %v", err)
	}
	if !strings.Contains(err.Error(), "quality=720") || !strings.Contains(err.Error(), "segment2.ts") {
		t.Errorf("the message must name the quality and the segment: %v", err)
	}
	// the body of the error page is cut, not dumped into the log
	if len(err.Error()) > 700 {
		t.Errorf("error message is too long (%d)", len(err.Error()))
	}

	if !f.hit("/m/hls/720/segment1.ts") || !f.hit("/m/hls/720/segment2.ts") {
		t.Error("segments before and including the missing one are requested")
	}
	for _, later := range []string{"/m/hls/720/segment3.ts", "/m/hls/720/segment4.ts", "/m/hls/720/segment5.ts", "/m/hls/480/i.m3u8"} {
		if f.hit(later) {
			t.Errorf("%s must not be requested after the 404", later)
		}
	}
}

func TestPermanentFailureUsesUpTheAttempts(t *testing.T) {
	f := newGoneFixture(t, func(string) bool { return false })
	defer f.stop()

	job := Job{FileID: 9, AttemptNumber: 1}
	if err := f.app.markFailed(context.Background(), job, &permanentError{err: io.EOF}); err != nil {
		t.Fatal(err)
	}
	if err := f.app.markFailed(context.Background(), job, io.EOF); err != nil {
		t.Fatal(err)
	}
	if len(f.rec.execs) != 2 {
		t.Fatalf("execs = %d", len(f.rec.execs))
	}
	if !strings.Contains(f.rec.execs[0].query, "attempts=GREATEST(attempts, 10)") {
		t.Errorf("permanent failure must use up the attempts:\n%s", f.rec.execs[0].query)
	}
	if strings.Contains(f.rec.execs[1].query, "GREATEST") {
		t.Errorf("an ordinary failure keeps the attempts:\n%s", f.rec.execs[1].query)
	}
	if got, _ := f.rec.execs[0].args[0].(string); !strings.HasPrefix(got, "permanent: ") {
		t.Errorf("last_error = %q", got)
	}
}

func TestOtherSourceErrorsStayPartial(t *testing.T) {
	// a 403 or 500 is not "gone": the quality is imported as far as it goes and the job is not permanent
	f := newGoneFixture(t, func(string) bool { return false })
	defer f.stop()
	if isSourceNotFound(&sourceStatusError{Status: 403}) || isSourceNotFound(&sourceStatusError{Status: 500}) || isSourceNotFound(io.EOF) {
		t.Fatal("only 404 is a missing file")
	}
	if !isSourceNotFound(&permanentError{err: &sourceStatusError{Status: 404}}) {
		t.Fatal("a wrapped 404 must still be recognised")
	}
}
