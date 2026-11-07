package main

import (
	"net/http"
	"net/url"
	"strings"
)

// The source (ashdi) rejects Go's default User-Agent with 403, so every request to it carries the headers of a
// browser, the same Chrome profile flixhub-mono uses for ashdi.
const (
	defaultSourceUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
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
	if strings.Contains(userAgent, "Chrome/140") {
		h.Set("Sec-CH-UA", `"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"`)
		h.Set("Sec-CH-UA-Mobile", "?0")
		h.Set("Sec-CH-UA-Platform", `"Windows"`)
	}
	if u, err := url.Parse(referer); err == nil && u.Scheme != "" && u.Host != "" {
		h.Set("Referer", referer)
		h.Set("Origin", u.Scheme+"://"+u.Host)
	}
	return h
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
