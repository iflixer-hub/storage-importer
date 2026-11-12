package main

import (
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
)

// The source (ashdi) rejects Go's default User-Agent with 403, so every request to it carries the headers of a
// browser, the same Chrome profile flixhub-mono uses for ashdi.
const (
	// Chrome/140.0.0.0 used to be here and the source started to answer 403 to exactly that string (other Chrome versions,
	// Firefox, Safari and curl pass), so the default is another version and the alternatives below are tried on a 403.
	defaultSourceUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36"
	defaultSourceReferer   = "https://uakino.best/"
	defaultSourceLanguage  = "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7"
)

// browserHeaders builds the header set of a media request made by a page on referer.
// Accept-Encoding is left out on purpose: Go adds gzip itself and unpacks the answer only then.
func browserHeaders(userAgent, referer, language string) http.Header {
	h := http.Header{}
	h.Set("User-Agent", userAgent)
	h.Set("Accept", "*/*")
	h.Set("Accept-Language", language)
	h.Set("Sec-Fetch-Dest", "empty")
	h.Set("Sec-Fetch-Mode", "cors")
	h.Set("Sec-Fetch-Site", "cross-site")
	if v := chromeMajor(userAgent); v != "" {
		h.Set("Sec-CH-UA", `"Chromium";v="`+v+`", "Not=A?Brand";v="24", "Google Chrome";v="`+v+`"`)
		h.Set("Sec-CH-UA-Mobile", "?0")
		h.Set("Sec-CH-UA-Platform", `"Windows"`)
	}
	if u, err := url.Parse(referer); err == nil && u.Scheme != "" && u.Host != "" {
		h.Set("Referer", referer)
		h.Set("Origin", u.Scheme+"://"+u.Host)
	}
	return h
}

var chromeVersion = regexp.MustCompile(`Chrome/(\d+)\.`)

// chromeMajor returns the Chrome version of a desktop Windows user agent ("" for any other): only that profile
// sends client hints, and they must match the user agent.
func chromeMajor(userAgent string) string {
	if !strings.Contains(userAgent, "Windows NT") {
		return ""
	}
	if m := chromeVersion.FindStringSubmatch(userAgent); m != nil {
		return m[1]
	}
	return ""
}

// defaultAlternativeUserAgents are tried one after another when the source refuses the user agent with 403.
var defaultAlternativeUserAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
}

// alternativeUserAgentsFromEnv reads SOURCE_USER_AGENT_ALT, user agents separated by "|" (they contain ";" and ",").
func alternativeUserAgentsFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv("SOURCE_USER_AGENT_ALT"))
	if raw == "" {
		return defaultAlternativeUserAgents
	}
	var list []string
	for _, ua := range strings.Split(raw, "|") {
		if ua = strings.TrimSpace(ua); ua != "" {
			list = append(list, ua)
		}
	}
	return list
}

// withUserAgent returns a copy of the request with another user agent. The client hints of the old one are dropped
// and those of the new one are set when it is a Windows Chrome, so the headers never contradict each other.
func withUserAgent(req *http.Request, userAgent string) *http.Request {
	out := req.Clone(req.Context())
	out.Header.Set("User-Agent", userAgent)
	out.Header.Del("Sec-CH-UA")
	out.Header.Del("Sec-CH-UA-Mobile")
	out.Header.Del("Sec-CH-UA-Platform")
	if v := chromeMajor(userAgent); v != "" {
		out.Header.Set("Sec-CH-UA", `"Chromium";v="`+v+`", "Not=A?Brand";v="24", "Google Chrome";v="`+v+`"`)
		out.Header.Set("Sec-CH-UA-Mobile", "?0")
		out.Header.Set("Sec-CH-UA-Platform", `"Windows"`)
	}
	return out
}

// browserHeadersFromEnv reads SOURCE_USER_AGENT, SOURCE_REFERER and SOURCE_ACCEPT_LANGUAGE.
func browserHeadersFromEnv() http.Header {
	return browserHeaders(
		envString("SOURCE_USER_AGENT", defaultSourceUserAgent),
		envString("SOURCE_REFERER", defaultSourceReferer),
		envString("SOURCE_ACCEPT_LANGUAGE", defaultSourceLanguage),
	)
}

// browserHeadersTransport adds the headers to every request that does not set them itself.
type browserHeadersTransport struct {
	base    http.RoundTripper
	headers http.Header
}

func (t *browserHeadersTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// a RoundTripper must not change the request it was given
	out := req.Clone(req.Context())
	for name, values := range t.headers {
		if out.Header.Get(name) != "" {
			continue
		}
		for _, v := range values {
			out.Header.Add(name, v)
		}
	}
	return t.base.RoundTrip(out)
}
