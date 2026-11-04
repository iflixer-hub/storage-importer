package main

import (
	"net/http"
	"net/url"
	"testing"
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
	t.Setenv("HTTPS_PROXY", "http://warp:1080")
	req, _ := http.NewRequest("GET", "https://ashdi.vip/x", nil)
	if u, _ := proxyForRequest(req); u == nil || u.Host != "warp:1080" {
		t.Errorf("without a pinned proxy the environment proxy is expected, got %v", u)
	}
	pinned, _ := url.Parse("http://u:p@isp.decodo.com:10004")
	req = req.WithContext(withSourceProxy(req.Context(), pinned))
	if u, _ := proxyForRequest(req); u == nil || u.Host != "isp.decodo.com:10004" || u.User == nil {
		t.Errorf("pinned proxy expected, got %v", u)
	}
}
