package main

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestQualityAllowed(t *testing.T) {
	const inf = "#EXT-X-STREAM-INF:RESOLUTION=854x430,BANDWIDTH=714000"
	cases := []struct {
		raw     string
		quality string
		inf     string
		want    bool
	}{
		{"", "1080", "", true},
		{"720,480", "720", "", true},
		{"720,480", "480", "", true},
		{"720,480", "1080", "", false},
		{" 720p ; 480 ", "480", "", true},
		{"430", "q1234567890", inf, true},
		{"480", "854p430", inf, false},
		{"720,480", "q1234567890", "", false},
	}
	for _, c := range cases {
		if got := qualityAllowed(parseQualityList(c.raw), c.quality, c.inf); got != c.want {
			t.Errorf("allowed=%q quality=%q: got %v, want %v", c.raw, c.quality, got, c.want)
		}
	}
}

func TestParseQualitiesJSON(t *testing.T) {
	if got := parseQualitiesJSON(`["480/index.m3u8","720/index.m3u8"]`); len(got) != 2 || got[0] != "480/index.m3u8" {
		t.Errorf("unexpected %v", got)
	}
	for _, raw := range []string{"", "null", "{}", "not json"} {
		if got := parseQualitiesJSON(raw); len(got) != 0 {
			t.Errorf("%q: expected empty, got %v", raw, got)
		}
	}
}

func TestParseProxyList(t *testing.T) {
	list := parseProxyList(`["http://u:p@isp.decodo.com:10004"," http://u:p@isp.decodo.com:10005","","bad","ftp://x:1"]`)
	if len(list) != 2 || list[0].Host != "isp.decodo.com:10004" || list[1].Port() != "10005" {
		t.Fatalf("unexpected %v", list)
	}
	if proxyEndpoint(list[0]) != "isp.decodo.com:10004" {
		t.Errorf("endpoint leaks more than host:port: %s", proxyEndpoint(list[0]))
	}
	for _, raw := range []string{"", "null", "{}", "garbage"} {
		if got := parseProxyList(raw); len(got) != 0 {
			t.Errorf("%q: expected empty, got %v", raw, got)
		}
	}
}

func TestProxyForRequest(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://ashdi.vip/x", nil)
	// ProxyFromEnvironment reads the environment once per process, so compare with it instead of setting variables
	got, _ := proxyForRequest(req)
	want, _ := http.ProxyFromEnvironment(req)
	if (got == nil) != (want == nil) || (got != nil && got.String() != want.String()) {
		t.Errorf("without a pinned proxy the environment proxy is expected: got %v, want %v", got, want)
	}
	pinned, _ := url.Parse("http://u:p@isp.decodo.com:10004")
	req = req.WithContext(withSourceProxy(req.Context(), pinned))
	if u, _ := proxyForRequest(req); u == nil || u.Host != "isp.decodo.com:10004" || u.User == nil {
		t.Errorf("pinned proxy expected, got %v", u)
	}
}

type fakeRT struct {
	mu    sync.Mutex
	times []time.Time
	codes []int
}

func (f *fakeRT) RoundTrip(*http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.times = append(f.times, time.Now())
	code := 200
	if len(f.codes) >= len(f.times) {
		code = f.codes[len(f.times)-1]
	}
	return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
}

func TestThrottleSpacesRequests(t *testing.T) {
	f := &fakeRT{}
	tr := newThrottledTransport(f, 50*time.Millisecond, 0)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("GET", "http://x/", nil)
			if _, err := tr.RoundTrip(req); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	// 5 requests, 4 gaps of at least 0.8*50ms
	if got := time.Since(start); got < 150*time.Millisecond {
		t.Errorf("requests were not spaced out: %v", got)
	}
	// another proxy has its own schedule
	other, _ := url.Parse("http://u:p@proxy2:1")
	req, _ := http.NewRequest("GET", "http://x/", nil)
	req = req.WithContext(withSourceProxy(req.Context(), other))
	s := time.Now()
	_, _ = tr.RoundTrip(req)
	if time.Since(s) > 20*time.Millisecond {
		t.Errorf("a different proxy must not wait for the first one")
	}
}

func TestThrottleRetriesOn429(t *testing.T) {
	f := &fakeRT{codes: []int{429, 429, 200}}
	tr := newThrottledTransport(f, 0, 3)
	tr.backoff = 10 * time.Millisecond
	req, _ := http.NewRequest("GET", "http://x/", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil || resp.StatusCode != 200 || len(f.times) != 3 {
		t.Fatalf("resp=%v err=%v calls=%d", resp, err, len(f.times))
	}
	if gap := f.times[2].Sub(f.times[1]); gap < 18*time.Millisecond {
		t.Errorf("backoff should double: %v", gap)
	}

	// retries exhausted: the 429 is handed back to the caller
	f = &fakeRT{codes: []int{429, 429, 429}}
	tr = newThrottledTransport(f, 0, 1)
	tr.backoff = time.Millisecond
	resp, _ = tr.RoundTrip(req)
	if resp.StatusCode != 429 || len(f.times) != 2 {
		t.Errorf("status=%d calls=%d", resp.StatusCode, len(f.times))
	}
}

func TestThrottleCallbacks(t *testing.T) {
	f := &fakeRT{codes: []int{429, 200}}
	tr := newThrottledTransport(f, 10*time.Millisecond, 2)
	tr.backoff = 5 * time.Millisecond
	limited := 0
	var waited time.Duration
	tr.onRateLimited = func(string) { limited++ }
	tr.onWait = func(_ string, d time.Duration) { waited += d }
	req, _ := http.NewRequest("GET", "http://x/", nil)
	if _, err := tr.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if limited != 1 || waited <= 0 {
		t.Errorf("limited=%d waited=%v", limited, waited)
	}
}

func TestProxyPoolSettingName(t *testing.T) {
	if got := newProxyPool(nil, time.Minute, "").setting; got != "ashdi_proxies" {
		t.Errorf("default setting = %q", got)
	}
	if got := newProxyPool(nil, time.Minute, "  luxhost_proxies ").setting; got != "luxhost_proxies" {
		t.Errorf("custom setting = %q", got)
	}
}

type captureRT struct{ got *http.Request }

func (c *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.got = r
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
}

func TestBrowserHeadersTransport(t *testing.T) {
	capture := &captureRT{}
	tr := &browserHeadersTransport{base: capture, headers: browserHeaders(defaultSourceUserAgent, "https://uakino.best/", defaultSourceLanguage)}

	req, _ := http.NewRequest("GET", "https://ashdi.vip/x/index.m3u8", nil)
	if _, err := tr.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	got := capture.got.Header
	if !strings.Contains(got.Get("User-Agent"), "Chrome/140") || strings.Contains(got.Get("User-Agent"), "Go-http-client") {
		t.Errorf("user agent = %q", got.Get("User-Agent"))
	}
	if got.Get("Referer") != "https://uakino.best/" || got.Get("Origin") != "https://uakino.best" {
		t.Errorf("referer/origin = %q / %q", got.Get("Referer"), got.Get("Origin"))
	}
	if got.Get("Accept-Encoding") != "" {
		t.Error("Accept-Encoding must stay unset so that Go unpacks gzip by itself")
	}
	if req.Header.Get("User-Agent") != "" {
		t.Error("the caller's request must not be modified")
	}

	// headers set by the caller win
	req, _ = http.NewRequest("GET", "https://ashdi.vip/x.ts", nil)
	req.Header.Set("User-Agent", "custom")
	if _, err := tr.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if capture.got.Header.Get("User-Agent") != "custom" {
		t.Errorf("user agent = %q", capture.got.Header.Get("User-Agent"))
	}
}

func TestBrowserHeadersFromEnv(t *testing.T) {
	t.Setenv("SOURCE_USER_AGENT", "Mozilla/5.0 Test")
	t.Setenv("SOURCE_REFERER", "https://example.org/page")
	h := browserHeadersFromEnv()
	if h.Get("User-Agent") != "Mozilla/5.0 Test" || h.Get("Referer") != "https://example.org/page" || h.Get("Origin") != "https://example.org" {
		t.Errorf("headers = %v", h)
	}
	if h.Get("Sec-CH-UA") != "" {
		t.Error("client hints belong to the Chrome profile only")
	}
	t.Setenv("SOURCE_USER_AGENT", "")
	t.Setenv("SOURCE_REFERER", "")
	if got := browserHeadersFromEnv().Get("Referer"); got != defaultSourceReferer {
		t.Errorf("default referer = %q", got)
	}
}

func TestClaimSelectSQL(t *testing.T) {
	with := claimSelectSQL(true)
	for _, want := range []string{"m3u8_errors", "CASE WHEN status='new' THEN m3u8_errors ELSE 0 END DESC", "FOR UPDATE SKIP LOCKED"} {
		if !strings.Contains(with, want) {
			t.Errorf("%q missing in\n%s", want, with)
		}
	}
	// new jobs still go before failed ones, the error count only orders inside the new group
	if strings.Index(with, "ORDER BY (status='failed')") < 0 || strings.Index(with, "(status='failed')") > strings.Index(with, "m3u8_errors ELSE") {
		t.Errorf("status order must come first:\n%s", with)
	}
	if strings.Contains(claimSelectSQL(false), "m3u8_errors") {
		t.Error("the fallback query must not mention the column")
	}
}

func TestIsUnknownColumn(t *testing.T) {
	if !isUnknownColumn(&mysqldriver.MySQLError{Number: 1054, Message: "Unknown column 'm3u8_errors' in 'field list'"}) {
		t.Error("1054 is an unknown column")
	}
	if isUnknownColumn(&mysqldriver.MySQLError{Number: 1213}) || isUnknownColumn(nil) || isUnknownColumn(io.EOF) {
		t.Error("only 1054 counts")
	}
}
