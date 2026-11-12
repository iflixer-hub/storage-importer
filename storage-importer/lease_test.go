package main

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

const leaseTableDDL = `CREATE TABLE IF NOT EXISTS importer_proxy_leases (
  proxy VARCHAR(255) NOT NULL,
  owner VARCHAR(128) NULL,
  leased_until DATETIME NOT NULL DEFAULT '2000-01-01 00:00:00',
  PRIMARY KEY (proxy)
) ENGINE=InnoDB`

func leaseTestPool(t *testing.T, n int) (*proxyPool, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN is not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{leaseTableDDL, "DELETE FROM importer_proxy_leases"} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	var list []*url.URL
	for i := 0; i < n; i++ {
		list = append(list, &url.URL{Scheme: "http", Host: "10.0.0." + string(rune('1'+i)) + ":50100"})
	}
	p := &proxyPool{db: db, ttl: time.Hour, loadedAt: time.Now(), list: list, leaseTTL: 3 * time.Second, leaseWait: 300 * time.Millisecond}
	return p, db
}

func TestLeaseGivesDistinctProxiesAndReusesReleased(t *testing.T) {
	p, _ := leaseTestPool(t, 3)
	ctx := context.Background()

	seen := map[string]bool{}
	var releases []func()
	for i := 0; i < 3; i++ {
		u, rel := p.acquire(ctx, "o"+string(rune('a'+i)))
		if seen[u.Host] {
			t.Fatalf("proxy %s was given twice", u.Host)
		}
		seen[u.Host] = true
		releases = append(releases, rel)
	}

	fallbacks := 0
	p.onFallback = func() { fallbacks++ }
	if u, rel := p.acquire(ctx, "od"); u == nil || fallbacks != 1 {
		t.Fatalf("with all proxies leased the job shares one after the wait (fallbacks=%d)", fallbacks)
	} else {
		rel()
	}

	releases[0]()
	u, rel := p.acquire(ctx, "oe")
	if fallbacks != 1 {
		t.Fatal("a released proxy must be taken without the fallback")
	}
	rel()
	_ = u
	releases[1]()
	releases[2]()
}

func TestLeaseConcurrentJobsNeverShare(t *testing.T) {
	p, _ := leaseTestPool(t, 6)
	ctx := context.Background()
	var mu sync.Mutex
	got := map[string]int{}
	var wg sync.WaitGroup
	var rels []func()
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u, rel := p.acquire(ctx, "w"+string(rune('a'+i)))
			mu.Lock()
			got[u.Host]++
			rels = append(rels, rel)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	for host, n := range got {
		if n != 1 {
			t.Errorf("proxy %s given to %d jobs at once", host, n)
		}
	}
	if len(got) != 6 {
		t.Errorf("used %d of 6 proxies", len(got))
	}
	for _, r := range rels {
		r()
	}
}

func TestLeaseExpiresAndIsRenewed(t *testing.T) {
	p, db := leaseTestPool(t, 1)
	p.leaseTTL = 2 * time.Second
	ctx := context.Background()

	_, rel := p.acquire(ctx, "x")
	// renewed every ttl/3: still leased after more than one ttl
	time.Sleep(3 * time.Second)
	var busy bool
	if err := db.QueryRow("SELECT leased_until > NOW() FROM importer_proxy_leases").Scan(&busy); err != nil || !busy {
		t.Fatalf("the lease must be renewed while held: busy=%v err=%v", busy, err)
	}
	rel()
	if err := db.QueryRow("SELECT leased_until > NOW() FROM importer_proxy_leases").Scan(&busy); err != nil || busy {
		t.Fatalf("release must free the proxy: busy=%v err=%v", busy, err)
	}

	// a lease nobody renews (dead process) frees itself
	if _, err := db.Exec("UPDATE importer_proxy_leases SET owner='dead', leased_until=DATE_ADD(NOW(), INTERVAL 1 SECOND)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	u, rel2 := p.acquire(ctx, "y")
	defer rel2()
	if u == nil {
		t.Fatal("expired lease must be reusable")
	}
}

func TestLeaseMissingTableFallsBackToRoundRobin(t *testing.T) {
	p, db := leaseTestPool(t, 2)
	if _, err := db.Exec("DROP TABLE importer_proxy_leases"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(leaseTableDDL) })
	u, rel := p.acquire(context.Background(), "z")
	defer rel()
	if u == nil {
		t.Fatal("the job must still get a proxy")
	}
	if p.leaseOffUntil.Load() == 0 {
		t.Error("leases must be switched off for a while")
	}
}

func TestLeaseOffWhenTTLIsZero(t *testing.T) {
	p, _ := leaseTestPool(t, 2)
	p.leaseTTL = 0
	u, rel := p.acquire(context.Background(), "z")
	defer rel()
	if u == nil {
		t.Fatal("round-robin must work with leases off")
	}
}

// 10 importer processes with 2 workers each against 30 proxies: 20 jobs at once get 20 different proxies
func TestLeaseManyProcessesNeverShare(t *testing.T) {
	_, db := leaseTestPool(t, 1)
	var list []*url.URL
	for i := 1; i <= 30; i++ {
		list = append(list, &url.URL{Scheme: "http", Host: "p" + strconv.Itoa(i) + ".example:50100"})
	}
	var mu sync.Mutex
	got := map[string]int{}
	var rels []func()
	var wg sync.WaitGroup
	for proc := 0; proc < 10; proc++ {
		pool := &proxyPool{db: db, ttl: time.Hour, loadedAt: time.Now(), list: list, leaseTTL: 30 * time.Second, leaseWait: time.Second}
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(proc, w int) {
				defer wg.Done()
				u, rel := pool.acquire(context.Background(), "n"+strconv.Itoa(proc)+"/"+strconv.Itoa(w))
				mu.Lock()
				got[u.Host]++
				rels = append(rels, rel)
				mu.Unlock()
			}(proc, w)
		}
	}
	wg.Wait()
	if len(got) != 20 {
		t.Errorf("20 jobs used %d different proxies", len(got))
	}
	for _, r := range rels {
		r()
	}
}
