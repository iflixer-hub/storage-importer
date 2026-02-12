package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	// ENV:
	// MYSQL_DSN="user:pass@tcp(host:3306)/db?parseTime=true&charset=utf8mb4"
	// R2_ACCOUNT_ID=...
	// R2_ACCESS_KEY=...
	// R2_SECRET_ACCESS_KEY=...
	// R2_BUCKET=...
	// R2_ENDPOINT="https://<accountid>.r2.cloudflarestorage.com"
	// WORKERS=1
	// CLEAR_FILES_PATH=true/false

	dsn := mustEnv("MYSQL_DSN")
	bucket := mustEnv("R2_BUCKET")
	endpoint := mustEnv("R2_ENDPOINT")
	ak := mustEnv("R2_ACCESS_KEY")
	sk := mustEnv("R2_SECRET_ACCESS_KEY")

	workers := envInt("WORKERS", 1)
	clearFiles := envBool("CLEAR_FILES_PATH", false)

	db, err := sql.Open("mysql", dsn)
	must(err)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(2 * time.Minute)

	httpClient := &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	must(err)

	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // важно для R2
	})

	app := &App{
		db:         db,
		httpClient: httpClient,
		s3:         s3c,
		uploader:   manager.NewUploader(s3c),

		bucket:     bucket,
		clearFiles: clearFiles,
		workers:    workers,
	}

	metrics := NewMetrics()
	app.metrics = metrics

	go serveMetrics(envString("METRICS_ADDR", ":9090"))

	log.Printf("start: workers=%d clearFiles=%v bucket=%s endpoint=%s", workers, clearFiles, bucket, endpoint)

	ctx := context.Background()
	for i := range workers {
		go app.worker(ctx, i)
	}
	select {}
}
