package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// listNames returns the names (keys without the prefix) of every object under prefix. It is one request per 1000
// objects, far cheaper than a HEAD for every chunk of a playlist.
func (a *App) listNames(ctx context.Context, prefix string) (map[string]bool, error) {
	names := make(map[string]bool)
	paginator := s3.NewListObjectsV2Paginator(a.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				names[strings.TrimPrefix(*obj.Key, prefix)] = true
			}
		}
	}
	return names, nil
}

// getObjectText reads a small text object; found is false when it does not exist.
func (a *App) getObjectText(ctx context.Context, key string) (text string, found bool, err error) {
	out, err := a.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(key)})
	if err != nil {
		if exists, headErr := a.headObject(ctx, key); headErr == nil && !exists {
			return "", false, nil
		}
		return "", false, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, 8<<20))
	if err != nil {
		return "", false, err
	}
	return string(body), true, nil
}

// playlistReferences lists the names a stored media playlist points to: its chunks and the URI of keys and maps.
func playlistReferences(playlist string) ([]string, error) {
	media, err := parseM3U8Media([]byte(playlist))
	if err != nil {
		return nil, err
	}
	var refs []string
	for _, l := range media.Lines {
		switch {
		case !l.IsTag && l.URI != "":
			refs = append(refs, l.URI)
		case l.IsTag && (strings.HasPrefix(l.Tag, "#EXT-X-KEY:") || strings.HasPrefix(l.Tag, "#EXT-X-MAP:")):
			if uri := extractURIFromTag(l.Tag); uri != "" {
				refs = append(refs, uri)
			}
		}
	}
	return refs, nil
}

// verifyQualityInStorage checks what the storage really holds for a quality of the job: its playlist and every chunk
// the playlist lists. complete is true only when all of them are there. names are the objects present in the quality
// folder, so the chunks that are already stored are not downloaded again.
func (a *App) verifyQualityInStorage(ctx context.Context, j Job, quality string) (complete bool, names map[string]bool, err error) {
	prefix := fmt.Sprintf("%s/%s/", j.R2Prefix, quality)
	names, err = a.listNames(ctx, prefix)
	if err != nil {
		return false, nil, fmt.Errorf("list %s: %w", prefix, err)
	}
	if !names["index.m3u8"] {
		return false, names, nil
	}

	playlist, found, err := a.getObjectText(ctx, prefix+"index.m3u8")
	if err != nil {
		return false, names, fmt.Errorf("read %sindex.m3u8: %w", prefix, err)
	}
	if !found {
		return false, names, nil
	}
	refs, err := playlistReferences(playlist)
	if err != nil || len(refs) == 0 {
		return false, names, nil
	}
	for _, ref := range refs {
		if !names[ref] {
			return false, names, nil
		}
	}
	return true, names, nil
}

// storedMasterEquals reports whether the master in the storage already has exactly this content.
func (a *App) storedMasterEquals(ctx context.Context, key, content string) bool {
	stored, found, err := a.getObjectText(ctx, key)
	return err == nil && found && stored == content
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
