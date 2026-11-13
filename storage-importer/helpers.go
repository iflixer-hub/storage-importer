package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
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

// parseQualityList turns "720,480" (commas or spaces, an optional "p" suffix) into a set. Empty means no limit.
func parseQualityList(raw string) map[string]struct{} {
	set := map[string]struct{}{}
	for _, item := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == ';' }) {
		item = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(item)), "p")
		if item != "" {
			set[item] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}

func sortedQualities(set map[string]struct{}) []string {
	list := make([]string, 0, len(set))
	for q := range set {
		list = append(list, q)
	}
	sort.Strings(list)
	return list
}

// variantHeight returns the height from "RESOLUTION=WxH" of a STREAM-INF line, or "".
func variantHeight(streamInf string) string {
	i := strings.Index(streamInf, "RESOLUTION=")
	if i < 0 {
		return ""
	}
	rest := streamInf[i+len("RESOLUTION="):]
	if j := strings.IndexAny(rest, ", \t"); j >= 0 {
		rest = rest[:j]
	}
	if x := strings.IndexAny(rest, "xX"); x >= 0 && isDigits(rest[x+1:]) {
		return rest[x+1:]
	}
	return ""
}

// qualityAllowed reports whether a variant passes the allow-list: either its folder label or its real height is listed.
func qualityAllowed(allowed map[string]struct{}, quality, streamInf string) bool {
	if len(allowed) == 0 {
		return true
	}
	if _, ok := allowed[quality]; ok {
		return true
	}
	if h := variantHeight(streamInf); h != "" {
		_, ok := allowed[h]
		return ok
	}
	return false
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

// m3u8ErrorsRecheck is how long jobs are claimed without m3u8_errors after the column turned out to be missing.
const m3u8ErrorsRecheck = 5 * time.Minute

// claimSelectSQL picks the next job. New jobs go first, and among them the ones the player found without a manifest in
// the storage (m3u8_errors, the most first); failed jobs follow, the least attempted first.
func claimSelectSQL(withM3u8Errors bool) string {
	if withM3u8Errors {
		return fmt.Sprintf(`
SELECT file_id, source_master_url, r2_prefix, r2_master_key, attempts, qualities_json, m3u8_errors
FROM files_storage
WHERE status IN ('new','failed')
  AND attempts < %d
ORDER BY (status='failed'), CASE WHEN status='new' THEN m3u8_errors ELSE 0 END DESC, attempts, file_id DESC
LIMIT 1
	FOR UPDATE SKIP LOCKED`, maxJobAttempts)
	}
	return fmt.Sprintf(`
SELECT file_id, source_master_url, r2_prefix, r2_master_key, attempts, qualities_json
FROM files_storage
WHERE status IN ('new','failed')
  AND attempts < %d
ORDER BY (status='failed'), attempts, file_id DESC
LIMIT 1
	FOR UPDATE SKIP LOCKED`, maxJobAttempts)
}

// isUnknownColumn reports MySQL error 1054 (a column of the query does not exist).
func isUnknownColumn(err error) bool {
	var me *mysqldriver.MySQLError
	return errors.As(err, &me) && me.Number == 1054
}

// maxJobAttempts is how many times a job is tried; a failed job with this many attempts is dead and never claimed.
const maxJobAttempts = 10

// sourceStatusError is a non-2xx answer of the source.
type sourceStatusError struct {
	URL    string
	Status int
	Body   string
}

func (e *sourceStatusError) Error() string {
	return fmt.Sprintf("GET %s status=%d body=%q", e.URL, e.Status, e.Body)
}

// sourceBodySnippet keeps the start of an error page (they are padded to kilobytes) on one line.
func sourceBodySnippet(body []byte) string {
	return truncate(strings.Join(strings.Fields(string(body)), " "), 200)
}

// isSourceNotFound reports a 404 from the source: the file (a segment, a playlist, a key) is gone there.
func isSourceNotFound(err error) bool {
	var se *sourceStatusError
	return errors.As(err, &se) && se.Status == http.StatusNotFound
}

// isSourceForbidden reports a 403 from the source.
func isSourceForbidden(err error) bool {
	var se *sourceStatusError
	return errors.As(err, &se) && se.Status == http.StatusForbidden
}

// isProxyFailure reports an error that the proxy rather than the source caused, or that another proxy may not
// have: the source refused this address (403), the proxy refused or could not pass the request (407, 5xx of the
// proxy), the connection failed or timed out.
func isProxyFailure(err error) bool {
	if err == nil {
		return false
	}
	var se *sourceStatusError
	if errors.As(err, &se) {
		switch se.Status {
		case http.StatusForbidden, http.StatusProxyAuthRequired, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// permanentError marks a failure that a retry cannot fix: the job is failed for good (attempts are used up).
type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func isPermanentError(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}
