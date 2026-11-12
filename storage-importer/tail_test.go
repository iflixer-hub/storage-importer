package main

import (
	"context"
	mysqldriver "github.com/go-sql-driver/mysql"
	dto "github.com/prometheus/client_model/go"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInTailWindow(t *testing.T) {
	cases := []struct {
		idx, total, tol int
		want            bool
	}{
		{100, 100, 30, true},
		{71, 100, 30, true},
		{70, 100, 30, false},
		{1, 100, 30, false},
		{5, 5, 30, true}, // the window is half of a short file: 2 chunks
		{4, 5, 30, true},
		{3, 5, 30, false},
		{2, 5, 30, false},
		{100, 100, 0, false}, // off
		{1, 1, 30, false},
	}
	for _, c := range cases {
		if got := inTailWindow(c.idx, c.total, c.tol); got != c.want {
			t.Errorf("inTailWindow(%d, %d, %d) = %v, want %v", c.idx, c.total, c.tol, got, c.want)
		}
	}
}

func TestDropSegmentTags(t *testing.T) {
	lines := []MediaLine{
		{IsTag: true, Tag: "#EXTM3U"},
		{IsTag: true, Tag: "#EXTINF:4,"},
		{URI: "1.ts"},
		{IsTag: true, Tag: "#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00Z"},
		{IsTag: true, Tag: "#EXTINF:4,"},
	}
	got := dropSegmentTags(lines)
	if len(got) != 3 || got[2].URI != "1.ts" {
		t.Fatalf("only the tags of the dropped segment go: %+v", got)
	}
}

func TestBrokenTailChunksDoNotFailTheFile(t *testing.T) {
	// 5 chunks: the window is the last 2, so 4 and 5 may be missing (404) or broken
	f := newGoneFixture(t, func(p string) bool {
		return strings.HasSuffix(p, "/720/segment4.ts") || strings.HasSuffix(p, "/720/segment5.ts")
	})
	defer f.stop()
	f.app.tailTolerance = 30

	before := testutilCounter(f.app.metrics.tailSegmentsSkipped)
	res, err := f.app.processJob(context.Background(), Job{FileID: 7, SourceMaster: f.srcURL + "/m/master.m3u8", R2Prefix: "files/7", R2MasterKey: "files/7/index.m3u8"})
	if err != nil {
		t.Fatalf("a broken tail must not fail the file: %v", err)
	}
	if res.partialMessage != "" {
		t.Errorf("the file must count as fully imported, got partial: %s", res.partialMessage)
	}
	if got := testutilCounter(f.app.metrics.tailSegmentsSkipped) - before; got != 2 {
		t.Errorf("skipped tail chunks = %v, want 2", got)
	}
}

func TestBrokenMiddleChunkStillFailsTheFile(t *testing.T) {
	f := newGoneFixture(t, func(p string) bool { return strings.HasSuffix(p, "/720/segment2.ts") })
	defer f.stop()
	f.app.tailTolerance = 30

	if _, err := f.app.processJob(context.Background(), Job{FileID: 7, SourceMaster: f.srcURL + "/m/master.m3u8", R2Prefix: "files/7", R2MasterKey: "files/7/index.m3u8"}); err == nil || !isSourceNotFound(err) {
		t.Fatalf("a chunk outside the tail still fails the file, got %v", err)
	}
}

func testutilCounter(c interface{ Write(*dto.Metric) error }) float64 {
	m := &dto.Metric{}
	_ = c.Write(m)
	return m.GetCounter().GetValue()
}

func TestReleaseJobReturnsTheJobWithoutSpendingTheAttempt(t *testing.T) {
	f := newGoneFixture(t, func(string) bool { return false })
	defer f.stop()

	if err := f.app.releaseJob(Job{FileID: 11, AttemptNumber: 3}); err != nil {
		t.Fatal(err)
	}
	if len(f.rec.execs) != 1 {
		t.Fatalf("execs = %d", len(f.rec.execs))
	}
	q := compactSQL(f.rec.execs[0].query)
	for _, want := range []string{"status='new'", "attempts=GREATEST(attempts-1, 0)", "status='in_progress' AND attempts=?"} {
		if !strings.Contains(q, want) {
			t.Errorf("query lacks %q:\n%s", want, q)
		}
	}
	if args := f.rec.execs[0].args; len(args) != 2 || args[0] != int64(11) || args[1] != int64(3) {
		t.Errorf("args = %v", args)
	}
}

func TestSleepCtxStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	sleepCtx(ctx, time.Minute)
	if time.Since(start) > time.Second {
		t.Error("a cancelled context must end the sleep at once")
	}
}

type proxyAwareRT struct {
	base http.RoundTripper
	mu   sync.Mutex
	used []string
	bad  string
}

func (r *proxyAwareRT) RoundTrip(req *http.Request) (*http.Response, error) {
	host := ""
	if u, ok := req.Context().Value(proxyCtxKey{}).(*url.URL); ok && u != nil {
		host = u.Host
	}
	if strings.HasSuffix(req.URL.Path, "master.m3u8") {
		r.mu.Lock()
		r.used = append(r.used, host)
		r.mu.Unlock()
		if host == r.bad {
			return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader("forbidden")), Header: http.Header{}, Request: req}, nil
		}
	}
	return r.base.RoundTrip(req)
}

func TestMasterForbiddenSwitchesToAnotherProxy(t *testing.T) {
	f := newGoneFixture(t, func(string) bool { return false })
	defer f.stop()
	rt := &proxyAwareRT{base: f.app.httpClient.Transport, bad: "bad.example:1"}
	if rt.base == nil {
		rt.base = http.DefaultTransport
	}
	f.app.httpClient = &http.Client{Transport: rt}
	f.app.masterProxySwitches = 3
	f.app.proxies.list = []*url.URL{{Scheme: "http", Host: "bad.example:1"}, {Scheme: "http", Host: "good.example:1"}}

	if _, err := f.app.processJob(context.Background(), Job{FileID: 5, SourceMaster: f.srcURL + "/m/master.m3u8", R2Prefix: "files/5", R2MasterKey: "files/5/index.m3u8"}); err != nil {
		t.Fatalf("the job must succeed through the second proxy: %v", err)
	}
	if len(rt.used) != 2 || rt.used[0] != "bad.example:1" || rt.used[1] != "good.example:1" {
		t.Fatalf("proxies used for the master: %v", rt.used)
	}

	// the refused proxy is passed over by the next jobs
	rt.used = nil
	if _, err := f.app.processJob(context.Background(), Job{FileID: 6, SourceMaster: f.srcURL + "/m/master.m3u8", R2Prefix: "files/6", R2MasterKey: "files/6/index.m3u8"}); err != nil {
		t.Fatal(err)
	}
	if len(rt.used) != 1 || rt.used[0] != "good.example:1" {
		t.Fatalf("a penalized proxy must be skipped: %v", rt.used)
	}
}

func TestMasterForbiddenEverywhereStillFails(t *testing.T) {
	f := newGoneFixture(t, func(string) bool { return false })
	defer f.stop()
	rt := &proxyAwareRT{base: http.DefaultTransport, bad: "bad.example:1"}
	f.app.httpClient = &http.Client{Transport: rt}
	f.app.masterProxySwitches = 3
	f.app.proxies.list = []*url.URL{{Scheme: "http", Host: "bad.example:1"}}

	_, err := f.app.processJob(context.Background(), Job{FileID: 5, SourceMaster: f.srcURL + "/m/master.m3u8"})
	if err == nil || !isSourceForbidden(err) {
		t.Fatalf("expected a 403 failure, got %v", err)
	}
	if len(rt.used) > 4 {
		t.Errorf("the switches are limited: %d attempts", len(rt.used))
	}
}

func TestUsableSkipsExcludedAndPenalized(t *testing.T) {
	a, b, c := &url.URL{Host: "a:1"}, &url.URL{Host: "b:1"}, &url.URL{Host: "c:1"}
	p := &proxyPool{}
	p.penalize(b)
	got := p.usable([]*url.URL{a, b, c}, []string{"a:1"})
	if len(got) != 1 || got[0].Host != "c:1" {
		t.Fatalf("got %v", got)
	}
	// nothing clean left: the job's own exclusions still hold, the penalty gives way
	got = p.usable([]*url.URL{a, b}, []string{"a:1"})
	if len(got) != 1 || got[0].Host != "b:1" {
		t.Fatalf("got %v", got)
	}
	// everything excluded: the whole list
	if got = p.usable([]*url.URL{a}, []string{"a:1"}); len(got) != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestRetryOnDeadlock(t *testing.T) {
	deadlock := &mysqldriver.MySQLError{Number: 1213, Message: "Deadlock"}
	calls := 0
	err := retryOnDeadlock(context.Background(), func() error {
		calls++
		if calls < 3 {
			return deadlock
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}

	calls = 0
	if err := retryOnDeadlock(context.Background(), func() error { calls++; return deadlock }); !isDeadlock(err) || calls != 3 {
		t.Fatalf("a deadlock that stays is returned after 3 tries: err=%v calls=%d", err, calls)
	}

	calls = 0
	other := io.EOF
	if err := retryOnDeadlock(context.Background(), func() error { calls++; return other }); err != other || calls != 1 {
		t.Fatalf("other errors are not retried: err=%v calls=%d", err, calls)
	}
}
