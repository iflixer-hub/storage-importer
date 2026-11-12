package main

import (
	"context"
	dto "github.com/prometheus/client_model/go"
	"strings"
	"testing"
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
