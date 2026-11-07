package main

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
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

// fakeS3 is a path-style S3 in memory: PUT, GET, HEAD and ListObjectsV2 of one bucket.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]string
	puts    []string
	srv     *httptest.Server
}

func newFakeS3(bucket string) *fakeS3 {
	f := &fakeS3{objects: map[string]string{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"+bucket), "/")
		switch {
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			f.objects[key] = string(body)
			f.puts = append(f.puts, key)
			w.Header().Set("ETag", `"x"`)
		case r.Method == http.MethodGet && key == "":
			prefix := r.URL.Query().Get("prefix")
			var keys []string
			for k := range f.objects {
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			var b strings.Builder
			b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
			for _, k := range keys {
				b.WriteString("<Contents><Key>" + k + "</Key></Contents>")
			}
			b.WriteString("</ListBucketResult>")
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, b.String())
		case r.Method == http.MethodGet, r.Method == http.MethodHead:
			body, ok := f.objects[key]
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				if r.Method == http.MethodGet {
					io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
				}
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			if r.Method == http.MethodGet {
				io.WriteString(w, body)
			}
		}
	}))
	return f
}

func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}
func (f *fakeS3) remove(key string) { f.mu.Lock(); delete(f.objects, key); f.mu.Unlock() }
func (f *fakeS3) putCount() int     { f.mu.Lock(); defer f.mu.Unlock(); return len(f.puts) }
func (f *fakeS3) resetPuts()        { f.mu.Lock(); f.puts = nil; f.mu.Unlock() }

type reuseFixture struct {
	app  *App
	s3   *fakeS3
	src  *httptest.Server
	mu   sync.Mutex
	reqs map[string]int // source path -> number of requests
}

func (f *reuseFixture) sourceRequests(suffix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for p, c := range f.reqs {
		if strings.HasSuffix(p, suffix) {
			n += c
		}
	}
	return n
}

func (f *reuseFixture) resetRequests() { f.mu.Lock(); f.reqs = map[string]int{}; f.mu.Unlock() }

func newReuseFixture(t *testing.T) *reuseFixture {
	t.Helper()
	f := &reuseFixture{reqs: map[string]int{}}
	f.src = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.reqs[r.URL.Path]++
		f.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "master.m3u8"):
			io.WriteString(w, "#EXTM3U\n#EXT-X-STREAM-INF:RESOLUTION=1280x720,BANDWIDTH=2\nhls/720/i.m3u8\n#EXT-X-STREAM-INF:RESOLUTION=854x480,BANDWIDTH=1\nhls/480/i.m3u8\n")
		case strings.HasSuffix(r.URL.Path, "i.m3u8"):
			io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg1.ts\n#EXTINF:4,\nseg2.ts\n#EXTINF:4,\nseg3.ts\n#EXT-X-ENDLIST\n")
		default:
			io.WriteString(w, "TS")
		}
	}))
	f.s3 = newFakeS3("b")

	recordingSeq++
	name := "recording" + strconv.Itoa(recordingSeq)
	sql.Register(name, &recordingDriver{})
	db, _ := sql.Open(name, "")

	cfg, _ := config.LoadDefaultConfig(context.Background(), config.WithRegion("r"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("a", "b", "")))
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(f.s3.srv.URL); o.UsePathStyle = true })
	f.app = &App{db: db, proxies: &proxyPool{db: db, ttl: time.Hour, loadedAt: time.Now()}, s3: s3c, uploader: manager.NewUploader(s3c),
		bucket: "b", metrics: sharedTestMetrics(), httpClient: f.src.Client()}
	t.Cleanup(func() { f.src.Close(); f.s3.srv.Close() })
	return f
}

func (f *reuseFixture) job(qualities []string) Job {
	return Job{FileID: 5, SourceMaster: f.src.URL + "/m/master.m3u8", R2Prefix: "files/5", R2MasterKey: "files/5/index.m3u8", Qualities: qualities}
}

func TestStoredFileIsNotImportedAgain(t *testing.T) {
	f := newReuseFixture(t)
	ctx := context.Background()

	// first import: everything goes to the storage
	if _, err := f.app.processJob(ctx, f.job(nil)); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"files/5/index.m3u8", "files/5/720/index.m3u8", "files/5/720/3.ts", "files/5/480/index.m3u8", "files/5/480/1.ts"} {
		if !f.s3.has(key) {
			t.Fatalf("%s missing after the first import", key)
		}
	}
	if got := f.sourceRequests(".ts"); got != 6 {
		t.Fatalf("first import downloaded %d chunks, want 6", got)
	}

	// second run, qualities_json empty (it was never saved): the storage is verified and nothing is downloaded or uploaded
	f.resetRequests()
	f.s3.resetPuts()
	if _, err := f.app.processJob(ctx, f.job(nil)); err != nil {
		t.Fatal(err)
	}
	if got := f.sourceRequests(".ts") + f.sourceRequests("i.m3u8"); got != 0 {
		t.Errorf("source was asked for %d chunks/playlists although everything is stored", got)
	}
	if got := f.s3.putCount(); got != 0 {
		t.Errorf("%d objects were uploaded again", got)
	}
	if got := f.sourceRequests("master.m3u8"); got != 1 {
		t.Errorf("the source master is read once, got %d", got)
	}
}

func TestOnlyMissingChunksAreDownloaded(t *testing.T) {
	f := newReuseFixture(t)
	ctx := context.Background()
	if _, err := f.app.processJob(ctx, f.job(nil)); err != nil {
		t.Fatal(err)
	}

	// one chunk of the 720 quality is lost
	f.s3.remove("files/5/720/2.ts")
	f.resetRequests()
	f.s3.resetPuts()
	if _, err := f.app.processJob(ctx, f.job(nil)); err != nil {
		t.Fatal(err)
	}
	if got := f.sourceRequests(".ts"); got != 1 {
		t.Errorf("only the lost chunk is downloaded, got %d chunk requests", got)
	}
	if got := f.sourceRequests("720/i.m3u8"); got != 1 {
		t.Errorf("the 720 playlist is read again, got %d", got)
	}
	if got := f.sourceRequests("480/i.m3u8"); got != 0 {
		t.Errorf("the intact 480 quality must not be touched, got %d", got)
	}
	if !f.s3.has("files/5/720/2.ts") {
		t.Error("the lost chunk must be back in the storage")
	}
}

func TestQualitiesJSONIsOnlyAHint(t *testing.T) {
	f := newReuseFixture(t)
	// the job says both qualities are done, but the bucket is empty (it was replaced)
	if _, err := f.app.processJob(context.Background(), f.job([]string{"720/index.m3u8", "480/index.m3u8"})); err != nil {
		t.Fatal(err)
	}
	if got := f.sourceRequests(".ts"); got != 6 {
		t.Errorf("an empty bucket means a full import, got %d chunk downloads", got)
	}
	if !f.s3.has("files/5/index.m3u8") || !f.s3.has("files/5/480/3.ts") {
		t.Error("the files must be imported again")
	}
}

func TestPlaylistReferences(t *testing.T) {
	refs, err := playlistReferences("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"enc.key\"\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4,\n1.ts\n#EXTINF:4,\n2.ts\n#EXT-X-ENDLIST\n")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(refs, ",") != "enc.key,init.mp4,1.ts,2.ts" {
		t.Errorf("refs = %v", refs)
	}
}
