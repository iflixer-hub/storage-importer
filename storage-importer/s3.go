package main

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (a *App) putObjectText(ctx context.Context, key, text string) error {
	body := strings.NewReader(text)
	_, err := a.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(a.bucket),
		Key:          aws.String(key),
		Body:         body,
		ContentType:  aws.String("application/vnd.apple.mpegurl"),
		CacheControl: aws.String("public, max-age=31536000, immutable"),
	})
	return err
}

func (a *App) headObject(ctx context.Context, key string) (bool, error) {
	_, err := a.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	var nsk *s3types.NotFound
	if errors.As(err, &nsk) {
		return false, nil
	}
	// R2 иногда возвращает generic error. Пробуем по тексту.
	if strings.Contains(strings.ToLower(err.Error()), "notfound") || strings.Contains(strings.ToLower(err.Error()), "not found") {
		return false, nil
	}
	return false, err
}
