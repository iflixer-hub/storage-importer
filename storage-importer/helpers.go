package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

/* -------------------- small utils -------------------- */

func resolveURL(base, ref string) string {
	// already absolute?
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref
	}
	bu, err := url.Parse(base)
	if err != nil {
		return ref
	}
	ru, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	return bu.ResolveReference(ru).String()
}

func looksLikeSegment(u string) bool {
	u = stripQuery(u)
	u = strings.ToLower(u)
	return strings.HasSuffix(u, ".ts")
}

func stripQuery(u string) string {
	if i := strings.Index(u, "?"); i >= 0 {
		return u[:i]
	}
	return u
}

func guessQualityFromURIOrInf(uri, streamInf string) string {
	// 1) если в uri есть "/480/" и т.п.
	parts := strings.Split(stripQuery(uri), "/")
	foundHlsPart := false
	for _, p := range parts {
		if p == "hls" {
			foundHlsPart = true
			continue
		}
		if !foundHlsPart {
			continue
		}
		// после "hls" может быть часть с цифрой, которая будет нашим условным quality label
		if isDigits(p) && len(p) >= 3 && len(p) <= 4 { // грубо: 480, 720, 1080
			return p
		}
	}
	// 2) если в инфе есть RESOLUTION=... можно вывести условный label
	if i := strings.Index(streamInf, "RESOLUTION="); i >= 0 {
		rest := streamInf[i+len("RESOLUTION="):]
		if j := strings.Index(rest, ","); j >= 0 {
			rest = rest[:j]
		}
		// rest типа 854x474
		return strings.ReplaceAll(rest, "x", "p") // грубо: 854p474 (можешь поменять)
	}
	return ""
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func shortHash(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:])[:10]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func mustEnv(k string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		log.Fatalf("missing env %s", k)
	}
	return v
}

func envInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func envBool(k string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	v = strings.ToLower(v)
	return v == "1" || v == "true" || v == "yes"
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type countingReader struct {
	r io.Reader
	n *int64
}

func (c countingReader) Read(p []byte) (int, error) {
	k, err := c.r.Read(p)
	atomic.AddInt64(c.n, int64(k))
	return k, err
}

func classifyKind(u string) string {
	u = strings.ToLower(stripQuery(u))
	if strings.HasSuffix(u, ".ts") {
		return "segment"
	}
	return "asset"
}

func logJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Print(err)
		return
	}
	log.Print(string(data))
}
