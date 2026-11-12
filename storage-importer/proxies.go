package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
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

	// A proxy is leased for the time of a job (importer_proxy_leases), so that no two jobs of any importer share
	// one. leaseTTL 0 turns it off. A lease is renewed every leaseTTL/3 and expires by itself if the process dies.
	leaseTTL  time.Duration
	leaseWait time.Duration // how long a job waits for a free proxy before it shares one
	// unix time until which the leases are not used (the table is missing or the database fails)
	leaseOffUntil atomic.Int64
	onFallback    func() // a job took a shared proxy because none was free

	leaseMu      sync.Mutex
	leaseRowsFor string // the list whose rows exist in the table
	penalties    map[string]time.Time
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

const (
	proxyLeasePoll = 2 * time.Second
	// a proxy that got a 403 on a master is passed over for this long
	proxyPenaltyTTL = 10 * time.Minute
)

// penalize marks a proxy as bad for a while: jobs take the others first.
func (p *proxyPool) penalize(u *url.URL) {
	if u == nil {
		return
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if p.penalties == nil {
		p.penalties = map[string]time.Time{}
	}
	p.penalties[u.Host] = time.Now().Add(proxyPenaltyTTL)
}

// usable drops the proxies that the job has already failed on (exclude) and those that are penalized. When nothing
// is left, the whole list is used: a bad proxy is better than no proxy.
func (p *proxyPool) usable(list []*url.URL, exclude []string) []*url.URL {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	now := time.Now()
	skip := make(map[string]bool, len(exclude))
	for _, h := range exclude {
		skip[h] = true
	}
	res := make([]*url.URL, 0, len(list))
	for _, u := range list {
		if skip[u.Host] || now.Before(p.penalties[u.Host]) {
			continue
		}
		res = append(res, u)
	}
	if len(res) == 0 {
		// keep the proxies this job has not failed on, penalized or not
		for _, u := range list {
			if !skip[u.Host] {
				res = append(res, u)
			}
		}
	}
	if len(res) == 0 {
		return list
	}
	return res
}

func isNoSuchTable(err error) bool {
	var me *mysqldriver.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}

// acquire returns a proxy for a job and the function that gives it back. The proxy that was released the longest
// ago is taken, so the load is spread over all of them. When every proxy is busy for leaseWait, or the lease table
// cannot be used, the round-robin choice is used (a proxy may then be shared) and the job is not stopped.
func (p *proxyPool) acquire(ctx context.Context, owner string, exclude ...string) (*url.URL, func()) {
	all := p.refresh(ctx)
	if len(all) == 0 {
		return nil, func() {}
	}
	list := p.usable(all, exclude)
	if p.leaseTTL > 0 && time.Now().Unix() >= p.leaseOffUntil.Load() {
		deadline := time.Now().Add(p.leaseWait)
		for {
			u, err := p.tryLease(ctx, list, owner)
			if err != nil {
				if ctx.Err() == nil {
					p.leaseOffUntil.Store(time.Now().Add(time.Minute).Unix())
					if isNoSuchTable(err) {
						p.leaseMu.Lock()
						p.leaseRowsFor = ""
						p.leaseMu.Unlock()
						log.Printf("[proxy] table importer_proxy_leases does not exist: proxies are shared round-robin for a minute")
					} else {
						log.Printf("[proxy] cannot lease a proxy: %v; proxies are shared round-robin for a minute", err)
					}
				}
				break
			}
			if u != nil {
				return u, p.holdLease(u, owner)
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				log.Printf("[proxy] no free proxy for %s, sharing one", p.leaseWait)
				if p.onFallback != nil {
					p.onFallback()
				}
				break
			}
			sleepCtx(ctx, proxyLeasePoll)
		}
	}
	return list[(p.rr.Add(1)-1)%uint64(len(list))], func() {}
}

// tryLease takes the free proxy that was used the longest ago (nil when all are busy). Rows are created here, so the
// table only has to exist. One UPDATE both finds and takes the proxy: a competing job waits for the row lock and then
// sees it taken, so two jobs never get the same one.
func (p *proxyPool) tryLease(ctx context.Context, list []*url.URL, owner string) (*url.URL, error) {
	byKey := make(map[string]*url.URL, len(list))
	keys := make([]string, 0, len(list))
	for _, u := range list {
		if _, dup := byKey[u.Host]; dup {
			continue
		}
		byKey[u.Host] = u
		keys = append(keys, u.Host)
	}
	sort.Strings(keys)

	if err := p.ensureRows(ctx, keys); err != nil {
		return nil, err
	}

	marks := strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",")
	args := []any{owner, int64(p.leaseTTL / time.Second)}
	for _, k := range keys {
		args = append(args, k)
	}

	var res sql.Result
	var err error
	// two jobs updating several rows can deadlock; MySQL aborts one of them and it is simply asked again
	for try := 0; try < 3; try++ {
		res, err = p.db.ExecContext(ctx, "UPDATE importer_proxy_leases SET owner=?, leased_until=DATE_ADD(NOW(), INTERVAL ? SECOND) WHERE proxy IN ("+marks+") AND leased_until < NOW() ORDER BY leased_until, RAND() LIMIT 1", args...)
		if !isDeadlock(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, nil
	}
	var key string
	if err := p.db.QueryRowContext(ctx, "SELECT proxy FROM importer_proxy_leases WHERE owner=? AND leased_until > NOW() ORDER BY leased_until DESC LIMIT 1", owner).Scan(&key); err != nil {
		return nil, err
	}
	return byKey[key], nil
}

func isDeadlock(err error) bool {
	var me *mysqldriver.MySQLError
	return errors.As(err, &me) && me.Number == 1213
}

// ensureRows creates the rows of the proxies that are not in the table yet; done once per list in this process.
func (p *proxyPool) ensureRows(ctx context.Context, keys []string) error {
	sig := strings.Join(keys, ",")
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if p.leaseRowsFor == sig {
		return nil
	}
	marks := strings.TrimSuffix(strings.Repeat("(?, '2000-01-01 00:00:00'),", len(keys)), ",")
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	if _, err := p.db.ExecContext(ctx, "INSERT IGNORE INTO importer_proxy_leases (proxy, leased_until) VALUES "+marks, args...); err != nil {
		return err
	}
	p.leaseRowsFor = sig
	return nil
}

// holdLease renews the lease until the returned function is called, which gives the proxy back. It runs on its own
// context: the job is cancelled when the process stops, and the proxy still has to be released then.
func (p *proxyPool) holdLease(u *url.URL, owner string) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(p.leaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				res, err := p.db.ExecContext(ctx, "UPDATE importer_proxy_leases SET leased_until=DATE_ADD(NOW(), INTERVAL ? SECOND) WHERE proxy=? AND owner=?", int64(p.leaseTTL/time.Second), u.Host, owner)
				cancel()
				if err != nil {
					log.Printf("[proxy] cannot renew the lease of %s: %v", u.Host, err)
				} else if n, _ := res.RowsAffected(); n == 0 {
					log.Printf("[proxy] the lease of %s was lost (it expired and was taken)", u.Host)
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := p.db.ExecContext(ctx, "UPDATE importer_proxy_leases SET leased_until=NOW(), owner=NULL WHERE proxy=? AND owner=?", u.Host, owner); err != nil {
			log.Printf("[proxy] cannot release %s: %v (it frees itself in %s)", u.Host, err, p.leaseTTL)
		}
	}
}

func proxyLeaseOwner(fileID uint64, attempt int) string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s/%d/%d", host, fileID, attempt)
}
