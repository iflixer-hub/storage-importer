package main

import "testing"

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
