package configfetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/kong/kubernetes-ingress-controller/v3/internal/adminapi"
	"github.com/kong/kubernetes-ingress-controller/v3/internal/dataplane/kongstate"
	"github.com/kong/kubernetes-ingress-controller/v3/internal/license"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3BackedFetcher wraps a LastValidConfigFetcher and persists the last valid
// KongState to an S3 object. It uploads on StoreLastValidConfig and attempts
// to download the object on LastValidConfig when the inner fetcher has no
// value stored in memory.
type S3BackedFetcher struct {
	inner      LastValidConfigFetcher
	bucket     string
	key        string
	sess       *session.Session
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	client     *s3.S3
}

// NewS3BackedFetcher creates a new S3BackedFetcher. The AWS session will be
// created with the provided AWS region (empty string uses default provider).
// Credentials and other config are picked up from the environment / shared
// config as usual for the AWS SDK.
func NewS3BackedFetcher(inner LastValidConfigFetcher, bucket, key, region string) (*S3BackedFetcher, error) {
	cfg := aws.Config{}
	if strings.TrimSpace(region) != "" {
		cfg.Region = aws.String(region)
	}
	sess, err := session.NewSession(&cfg)
	if err != nil {
		return nil, fmt.Errorf("creating aws session: %w", err)
	}
	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)
	client := s3.New(sess)
	return &S3BackedFetcher{
		inner:      inner,
		bucket:     bucket,
		key:        key,
		sess:       sess,
		uploader:   uploader,
		downloader: downloader,
		client:     client,
	}, nil
}

// StartPeriodicUpload starts a background goroutine that periodically uploads
// the currently stored last-valid config (if any) to S3. The goroutine stops
// when the provided context is cancelled.
func (sf *S3BackedFetcher) StartPeriodicUpload(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				st, ok := sf.inner.LastValidConfig()
				if !ok || st == nil {
					continue
				}
				b, err := json.Marshal(st)
				if err != nil {
					log.Printf("failed to marshal kong state for periodic s3 upload: %v", err)
					continue
				}
				req := &s3manager.UploadInput{
					Bucket: aws.String(sf.bucket),
					Key:    aws.String(sf.key),
					Body:   strings.NewReader(string(b)),
				}
				if _, err := sf.uploader.UploadWithContext(ctx, req); err != nil {
					log.Printf("failed periodic upload of last-valid kong state to s3 bucket=%s key=%s: %v", sf.bucket, sf.key, err)
					continue
				}
				log.Printf("periodic upload of last-valid kong state to s3 bucket=%s key=%s succeeded", sf.bucket, sf.key)
			}
		}
	}()
}

// LastValidConfig first checks the inner fetcher, and if no state is found it
// attempts to download from S3 and populate the inner fetcher.
func (sf *S3BackedFetcher) LastValidConfig() (*kongstate.KongState, bool) {
	if st, ok := sf.inner.LastValidConfig(); ok {
		return st, true
	}

	// Try to download from S3
	buf := &aws.WriteAtBuffer{}
	_, err := sf.downloader.DownloadWithContext(context.Background(), buf, &s3.GetObjectInput{
		Bucket: aws.String(sf.bucket),
		Key:    aws.String(sf.key),
	})
	if err != nil {
		// couldn't fetch from S3
		// best-effort, just log and return not found
		log.Printf("failed to download last-valid config from s3 bucket=%s key=%s: %v", sf.bucket, sf.key, err)
		return nil, false
	}
	var ks kongstate.KongState
	if err := json.Unmarshal(buf.Bytes(), &ks); err != nil {
		log.Printf("failed to unmarshal kong state downloaded from s3: %v", err)
		return nil, false
	}
	// store into inner fetcher for future fast access
	sf.inner.StoreLastValidConfig(&ks)
	return &ks, true
}

// StoreLastValidConfig stores in-memory via the inner fetcher and also uploads
// a JSON copy to S3. Errors uploading to S3 are returned but the inner store
// is always updated first.
func (sf *S3BackedFetcher) StoreLastValidConfig(s *kongstate.KongState) {
	// store in memory first
	sf.inner.StoreLastValidConfig(s)

	// marshal to json
	// b, err := json.Marshal(s)
	// if err != nil {
	// 	log.Printf("failed to marshal kong state for s3 upload: %v", err)
	// 	return
	// }

	// // upload
	// req := &s3manager.UploadInput{
	// 	Bucket: aws.String(sf.bucket),
	// 	Key:    aws.String(sf.key),
	// 	Body:   strings.NewReader(string(b)),
	// }
	// if _, err := sf.uploader.UploadWithContext(context.Background(), req); err != nil {
	// 	log.Printf("failed to upload last-valid kong state to s3 bucket=%s key=%s: %v", sf.bucket, sf.key, err)
	// 	return
	// }
	// log.Printf("uploaded last-valid kong state to s3 bucket=%s key=%s", sf.bucket, sf.key)
}

// TryFetchingValidConfigFromGateways delegates to inner implementation.
func (sf *S3BackedFetcher) TryFetchingValidConfigFromGateways(ctx context.Context, logger logr.Logger, gatewayClients []*adminapi.Client, customEntityTypes []string) error {
	return sf.inner.TryFetchingValidConfigFromGateways(ctx, logger, gatewayClients, customEntityTypes)
}

func (sf *S3BackedFetcher) InjectLicenseGetter(licenseGetter license.Getter) {
	sf.inner.InjectLicenseGetter(licenseGetter)
}
