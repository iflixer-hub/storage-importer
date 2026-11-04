package main

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// throttledTransport spaces out requests to the source and backs off on 429, per proxy address
// (the source limits by IP, and all jobs on the same proxy share that limit).
type throttledTransport struct {
	base       http.RoundTripper
	gap        time.Duration // pause between request starts on one proxy; 0 disables it
	maxRetries int           // retries after a 429
	backoff    time.Duration // first wait after a 429 when the response has no Retry-After, doubles each retry

	mu   sync.Mutex
	next map[string]time.Time
}

const maxRateLimitWait = 60 * time.Second

func newThrottledTransport(base http.RoundTripper, gap time.Duration, maxRetries int) *throttledTransport {
	return &throttledTransport{base: base, gap: gap, maxRetries: maxRetries, backoff: 5 * time.Second, next: map[string]time.Time{}}
}

func throttleKey(req *http.Request) string {
	if u, ok := req.Context().Value(proxyCtxKey{}).(*url.URL); ok && u != nil {
		return u.Host
	}
	return "env"
}

func (t *throttledTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	key := throttleKey(req)
	for attempt := 0; ; attempt++ {
		if err := t.wait(req.Context(), key); err != nil {
			return nil, err
		}
		resp, err := t.base.RoundTrip(req)
		if err != nil || resp.StatusCode != http.StatusTooManyRequests || attempt >= t.maxRetries {
			return resp, err
		}

		pause := retryAfter(resp, t.backoff<<attempt)
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		// every job on this proxy waits, not just this request
		t.penalize(key, pause)
	}
}

// wait reserves the next free slot for key and sleeps until it comes.
func (t *throttledTransport) wait(ctx context.Context, key string) error {
	t.mu.Lock()
	now := time.Now()
	slot := t.next[key]
	if slot.Before(now) {
		slot = now
	}
	t.next[key] = slot.Add(t.jitter())
	t.mu.Unlock()

	d := time.Until(slot)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *throttledTransport) penalize(key string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if until := time.Now().Add(d); until.After(t.next[key]) {
		t.next[key] = until
	}
}

// jitter keeps parallel workers from hitting the source in lockstep: gap ±20%.
func (t *throttledTransport) jitter() time.Duration {
	if t.gap <= 0 {
		return 0
	}
	return time.Duration(float64(t.gap) * (0.8 + 0.4*rand.Float64()))
}

func retryAfter(resp *http.Response, fallback time.Duration) time.Duration {
	d := fallback
	if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
		d = time.Duration(secs) * time.Second
	}
	if d > maxRateLimitWait {
		d = maxRateLimitWait
	}
	return d
}
