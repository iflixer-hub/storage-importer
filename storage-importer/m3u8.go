package main

import (
	"bufio"
	"bytes"
	"strings"
)

/* -------------------- M3U8 parsing/rendering -------------------- */

type Master struct {
	HeaderLines []string
	Variants    []MasterVariant
}

type MasterVariant struct {
	StreamInfLine string // "#EXT-X-STREAM-INF:..."
	URI           string // next line
	OtherLines    []string
}

func parseM3U8Master(b []byte) (Master, error) {
	s := bufio.NewScanner(bytes.NewReader(b))
	m := Master{}
	var pendingInf string
	var other []string

	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		if line == "#EXTM3U" || strings.HasPrefix(line, "#EXT-X-VERSION") {
			m.HeaderLines = append(m.HeaderLines, line)
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-STREAM-INF:") {
			pendingInf = line
			continue
		}
		if pendingInf != "" && !strings.HasPrefix(line, "#") {
			m.Variants = append(m.Variants, MasterVariant{
				StreamInfLine: pendingInf,
				URI:           line,
				OtherLines:    other,
			})
			pendingInf = ""
			other = nil
			continue
		}
		// прочие теги просто сохраним
		if strings.HasPrefix(line, "#") {
			other = append(other, line)
			continue
		}
		// если видим uri без stream-inf → скорее всего это media playlist, а не master
		// тогда variants оставляем пустыми (caller обработает как media)
	}
	if err := s.Err(); err != nil {
		return Master{}, err
	}
	return m, nil
}

func (m Master) Render() string {
	var out strings.Builder
	out.WriteString("#EXTM3U\n")
	for _, h := range m.HeaderLines {
		if h == "#EXTM3U" {
			continue
		}
		out.WriteString(h + "\n")
	}
	for _, v := range m.Variants {
		for _, l := range v.OtherLines {
			out.WriteString(l + "\n")
		}
		out.WriteString(v.StreamInfLine + "\n")
		out.WriteString(v.URI + "\n")
	}
	return out.String()
}

type Media struct {
	Lines     []MediaLine
	HasExtM3U bool
}

type MediaLine struct {
	IsTag bool
	Tag   string
	URI   string
}

func (m Media) Render() string {
	var out strings.Builder
	out.WriteString("#EXTM3U\n")
	for _, l := range m.Lines {
		if l.IsTag {
			out.WriteString(l.Tag + "\n")
		} else {
			out.WriteString(l.URI + "\n")
		}
	}
	return out.String()
}

func parseM3U8Media(b []byte) (Media, error) {
	s := bufio.NewScanner(bytes.NewReader(b))
	m := Media{}
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		if line == "#EXTM3U" {
			m.HasExtM3U = true
			continue
		}
		if strings.HasPrefix(line, "#") {
			m.Lines = append(m.Lines, MediaLine{IsTag: true, Tag: line})
		} else {
			m.Lines = append(m.Lines, MediaLine{IsTag: false, URI: line})
		}
	}
	return m, s.Err()
}
