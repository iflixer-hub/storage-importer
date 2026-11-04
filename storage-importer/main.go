package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	debugMySQL     bool
	mysqlLogErrors bool
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
	region := mustEnv("R2_REGION")
	endpoint := mustEnv("R2_ENDPOINT")
	ak := mustEnv("R2_ACCESS_KEY")
	// skFile := mustEnv("R2_SECRET_ACCESS_KEY_FILE")
	// sk, err := os.ReadFile(skFile)
	// must(err)
	// skStr := strings.TrimSpace(string(sk))
	skStr := mustEnv("R2_SECRET_ACCESS_KEY")

	workers := envInt("WORKERS", 1)
	clearFiles := envBool("CLEAR_FILES_PATH", false)
	allowedQualities := parseQualityList(os.Getenv("ALLOWED_QUALITIES"))
	jobHeartbeatInterval := time.Duration(envInt("JOB_HEARTBEAT_SECONDS", 60)) * time.Second
	jobRecoveryTimeout := time.Duration(envInt("JOB_RECOVERY_TIMEOUT_SECONDS", 600)) * time.Second
	jobRecoveryInterval := time.Duration(envInt("JOB_RECOVERY_INTERVAL_SECONDS", 60)) * time.Second
	if jobHeartbeatInterval <= 0 || jobRecoveryInterval <= 0 || jobRecoveryTimeout <= 2*jobHeartbeatInterval {
		log.Fatal("JOB_RECOVERY_TIMEOUT_SECONDS must exceed twice JOB_HEARTBEAT_SECONDS; all recovery values must be positive")
	}
	debugMySQL = envBool("DEBUG_MYSQL", false)
	mysqlLogErrors = envBool("MYSQL_LOG_ERRORS", true)

	db, err := sql.Open("mysql", dsn)
	must(err)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(2 * time.Minute)

	sourceTransport := &http.Transport{
		Proxy:               proxyForRequest,
		ForceAttemptHTTP2:   false,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	}
	httpClient := &http.Client{
		Timeout: 120 * time.Second,
		Transport: newThrottledTransport(
			sourceTransport,
			time.Duration(envInt("SOURCE_REQUEST_DELAY_MS", 0))*time.Millisecond,
			envInt("SOURCE_429_RETRIES", 3),
		),
	}
	r2Transport := http.DefaultTransport.(*http.Transport).Clone()
	r2Transport.Proxy = nil
	r2HTTPClient := &http.Client{
		Timeout:   120 * time.Second,
		Transport: r2Transport,
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, skStr, "")),
		config.WithHTTPClient(r2HTTPClient),
	)
	must(err)

	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // важно для R2
	})

	app := &App{
		db:         db,
		httpClient: httpClient,
		proxies:    newProxyPool(db, proxyListTTL),
		s3:         s3c,
		uploader:   manager.NewUploader(s3c),

		bucket:     bucket,
		clearFiles: clearFiles,

		allowedQualities: allowedQualities,
		workers:          workers,

		jobHeartbeatInterval: jobHeartbeatInterval,
		jobRecoveryTimeout:   jobRecoveryTimeout,
		jobRecoveryInterval:  jobRecoveryInterval,
	}

	metrics := NewMetrics()
	app.metrics = metrics

	go startDBStatsLoop(context.Background(), db, metrics, 30*time.Second)

	go serveMetrics(envString("METRICS_ADDR", ":9090"))

	log.Printf("start: sourceDelayMs=%d source429Retries=%d allowedQualities=%v workers=%d clearFiles=%v bucket=%s endpoint=%s debugMySQL=%v mysqlLogErrors=%v", envInt("SOURCE_REQUEST_DELAY_MS", 0), envInt("SOURCE_429_RETRIES", 3), sortedQualities(allowedQualities), workers, clearFiles, bucket, endpoint, debugMySQL, mysqlLogErrors)

	ctx := context.Background()
	go app.recoveryLoop(ctx)
	for i := range workers {
		go app.worker(ctx, i)
	}
	select {}
}
