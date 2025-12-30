package store

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	yamlserializer "k8s.io/apimachinery/pkg/runtime/serializer/yaml"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/kong/kubernetes-ingress-controller/v3/pkg/manager/scheme"
)

// S3CachePersister persists CacheStores snapshots to S3 and can restore them.
// It is intentionally simple: snapshots are stored as concatenated YAML documents
// (one per object) in a single object on S3.
type S3CachePersister struct {
	bucket     string
	key        string
	sess       *session.Session
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

// NewS3CachePersister creates a new persister. Region may be empty to use default.
func NewS3CachePersister(bucket, key, region string) (*S3CachePersister, error) {
	cfg := aws.Config{}
	if strings.TrimSpace(region) != "" {
		cfg.Region = aws.String(region)
	}
	sess, err := session.NewSession(&cfg)
	if err != nil {
		return nil, fmt.Errorf("creating aws session: %w", err)
	}
	return &S3CachePersister{
		bucket:     bucket,
		key:        key,
		sess:       sess,
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
	}, nil
}

// SaveSnapshot serializes the provided snapshot and uploads it to S3.
func (p *S3CachePersister) SaveSnapshot(ctx context.Context, snapshot CacheStores) error {
	// take a deep snapshot if caller provided the live CacheStores
	var snap CacheStores
	// If snapshot has a lock, use its TakeSnapshot, otherwise assume provided snapshot is already a snapshot.
	if snapshot.l != nil {
		var err error
		snap, err = snapshot.TakeSnapshot()
		if err != nil {
			return fmt.Errorf("taking snapshot: %w", err)
		}
	} else {
		snap = snapshot
	}

	buf := bytes.Buffer{}
	sy := serializer.NewYAMLSerializer(yamlserializer.DefaultMetaFactory, scheme.Get(), scheme.Get())

	for _, store := range snap.ListAllStores() {
		for _, item := range store.List() {
			obj, ok := item.(runtime.Object)
			if !ok {
				continue
			}
			// encode each object as YAML document
			if err := sy.Encode(obj, &buf); err != nil {
				log.Printf("failed to encode object for s3 snapshot: %v", err)
				continue
			}
			buf.WriteString("---\n")
		}
	}

	req := &s3manager.UploadInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.key),
		Body:   bytes.NewReader(buf.Bytes()),
	}
	if _, err := p.uploader.UploadWithContext(ctx, req); err != nil {
		return fmt.Errorf("uploading snapshot to s3: %w", err)
	}
	return nil
}

// LoadSnapshot downloads the S3 object, decodes YAML docs and returns a CacheStores
// constructed from them.
func (p *S3CachePersister) LoadSnapshot(ctx context.Context) (CacheStores, error) {
	buf := &aws.WriteAtBuffer{}
	_, err := p.downloader.DownloadWithContext(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.key),
	})
	if err != nil {
		return CacheStores{}, fmt.Errorf("downloading snapshot from s3: %w", err)
	}

	// split into YAML docs by the document separator
	content := buf.Bytes()
	docs := splitYAMLDocs(content)
	if len(docs) == 0 {
		return CacheStores{}, fmt.Errorf("no YAML docs found in snapshot")
	}

	return NewCacheStoresFromObjYAML(docs...)
}

func splitYAMLDocs(b []byte) [][]byte {
	text := string(b)
	parts := strings.Split(text, "---\n")
	out := make([][]byte, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, []byte(p))
	}
	return out
}

// StartPeriodicSave uploads snapshots periodically. It will call SaveSnapshot
// using the provided live cache every interval; the goroutine stops when ctx is done.
func (p *S3CachePersister) StartPeriodicSave(ctx context.Context, interval time.Duration, cache CacheStores) {
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
				if err := p.SaveSnapshot(ctx, cache); err != nil {
					log.Printf("failed to periodic save cache snapshot to s3: %v", err)
				}
			}
		}
	}()
}
