package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The proxies come from a row of the `settings` table, a JSON list like ["http://user:pass@host:10004", ...].
// ashdi_proxies is the row flixhub-mono reads for its ashdi requests; SOURCE_PROXIES_SETTING picks another one
// (for example luxhost_proxies).
const (
	defaultProxiesSetting = "ashdi_proxies"
	proxyListTTL          = 10 * time.Minute
)

type proxyCtxKey struct{}

// proxyPool hands out the proxies from the settings table round-robin. The list is cached,
// and the previous one is kept when the database cannot be read.
type proxyPool struct {
	db      *sql.DB
	ttl     time.Duration
	setting string // name of the settings row

	mu       sync.Mutex
	list     []*url.URL
	loadedAt time.Time

	rr atomic.Uint64
}

func newProxyPool(db *sql.DB, ttl time.Duration, setting string) *proxyPool {
	setting = strings.TrimSpace(setting)
	if setting == "" {
		setting = defaultProxiesSetting
	}
	return &proxyPool{db: db, ttl: ttl, setting: setting}
}

func parseProxyList(raw string) []*url.URL {
	var items []string
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &items) != nil {
		return nil
	}
	list := make([]*url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(strings.TrimSpace(item))
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5") {
			continue
		}
		list = append(list, u)
	}
	return list
}

func (p *proxyPool) refresh(ctx context.Context) []*url.URL {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.loadedAt.IsZero() && time.Since(p.loadedAt) < p.ttl {
		return p.list
	}

	var raw sql.NullString
	err := p.db.QueryRowContext(ctx, "SELECT value FROM settings WHERE name=? LIMIT 1", p.setting).Scan(&raw)
	switch {
	case err == nil:
		p.list = parseProxyList(raw.String)
	case err == sql.ErrNoRows:
		p.list = nil
	default:
		// keep what we had, but retry sooner than a full ttl
		log.Printf("[proxy] cannot read setting %s: %v", p.setting, err)
		if p.loadedAt.IsZero() {
			p.loadedAt = time.Now().Add(-p.ttl + 30*time.Second)
		}
		return p.list
	}
	p.loadedAt = time.Now()
	log.Printf("[proxy] %s: %d proxies", p.setting, len(p.list))
	return p.list
}

// next returns the next proxy, or nil when the setting is empty (the environment proxy is used then).
func (p *proxyPool) next(ctx context.Context) *url.URL {
	list := p.refresh(ctx)
	if len(list) == 0 {
		return nil
	}
	return list[(p.rr.Add(1)-1)%uint64(len(list))]
}

// withSourceProxy pins a proxy to every request made with the returned context, so one job
// reads all its playlists and segments through the same address.
func withSourceProxy(ctx context.Context, u *url.URL) context.Context {
	if u == nil {
		return ctx
	}
	return context.WithValue(ctx, proxyCtxKey{}, u)
}

// proxyForRequest is the Transport.Proxy of the source client: the proxy pinned to the job,
// otherwise the HTTP(S)_PROXY environment (warp).
func proxyForRequest(req *http.Request) (*url.URL, error) {
	if u, ok := req.Context().Value(proxyCtxKey{}).(*url.URL); ok && u != nil {
		return u, nil
	}
	return http.ProxyFromEnvironment(req)
}

// proxyEndpoint is host:port only, credentials must never reach the logs.
func proxyEndpoint(u *url.URL) string {
	if u == nil {
		return "env"
	}
	return u.Host
}
