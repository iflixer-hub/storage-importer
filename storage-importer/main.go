package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"log"
	"net/http"
	"os"
	"strings"
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
	skFile := mustEnv("R2_SECRET_ACCESS_KEY_FILE")
	sk, err := os.ReadFile(skFile)
	must(err)
	skStr := strings.TrimSpace(string(sk))

	workers := envInt("WORKERS", 1)
	clearFiles := envBool("CLEAR_FILES_PATH", false)

	db, err := sql.Open("mysql", dsn)
	must(err)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(2 * time.Minute)

	httpClient := &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   false,
			TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, skStr, "")),
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

	go startDBStatsLoop(context.Background(), db, metrics, 30*time.Second)

	go serveMetrics(envString("METRICS_ADDR", ":9090"))

	log.Printf("start: workers=%d clearFiles=%v bucket=%s endpoint=%s", workers, clearFiles, bucket, endpoint)

	ctx := context.Background()
	for i := range workers {
		go app.worker(ctx, i)
	}
	select {}
}
