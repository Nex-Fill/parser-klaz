package media

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/disintegration/imaging"
	"github.com/panjf2000/ants/v2"
	"github.com/rs/zerolog/log"

	"github.com/danamakarenko/klaz-parser/internal/config"
	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

type Pipeline struct {
	s3Client        *s3.Client
	bucketOriginals string
	bucketPreviews  string
	cdnBase         string
	previewWidth    int
	pool            *ants.Pool
	httpClient      *http.Client
}

func NewPipeline(cfg config.S3Config, workerCount int) (*Pipeline, error) {
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               fmt.Sprintf("https://%s", cfg.Endpoint),
			SigningRegion:      cfg.Region,
			HostnameImmutable: true,
		}, nil
	})

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKey, cfg.SecretKey, "",
		)),
		awsconfig.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	pool, err := ants.NewPool(workerCount, ants.WithPreAlloc(true))
	if err != nil {
		return nil, err
	}

	return &Pipeline{
		s3Client:        s3.NewFromConfig(awsCfg),
		bucketOriginals: cfg.BucketOriginals,
		bucketPreviews:  cfg.BucketPreviews,
		cdnBase:         cfg.CDNBase,
		previewWidth:    cfg.PreviewWidth,
		pool:            pool,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
			},
		},
	}, nil
}

func (p *Pipeline) Close() {
	p.pool.Release()
}

func (p *Pipeline) ProcessImages(ctx context.Context, adID string, urls []string) ([]kl.AdImage, error) {
	var mu sync.Mutex
	var images []kl.AdImage
	var wg sync.WaitGroup

	for i, imgURL := range urls {
		if i >= 10 {
			break
		}
		pos := i
		imgURL := imgURL
		wg.Add(1)
		p.pool.Submit(func() {
			defer wg.Done()
			img, err := p.processOne(ctx, adID, imgURL, pos)
			if err != nil {
				log.Warn().Err(err).Str("url", imgURL).Msg("image process failed")
				return
			}
			mu.Lock()
			images = append(images, *img)
			mu.Unlock()
		})
	}

	wg.Wait()
	return images, nil
}

func (p *Pipeline) processOne(ctx context.Context, adID, imgURL string, position int) (*kl.AdImage, error) {
	data, err := p.download(ctx, imgURL)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}

	hash := kl.ImageHash(data)
	ext := getExtension(imgURL)
	originalKey := fmt.Sprintf("%s.%s", hash, ext)
	previewKey := fmt.Sprintf("%s_preview.jpg", hash)

	contentType := "image/" + ext
	if ext == "jpg" {
		contentType = "image/jpeg"
	}

	if !p.objectExists(ctx, p.bucketOriginals, originalKey) {
		if err := p.upload(ctx, p.bucketOriginals, originalKey, data, contentType); err != nil {
			return nil, fmt.Errorf("upload original: %w", err)
		}
	}

	if position == 0 && !p.objectExists(ctx, p.bucketPreviews, previewKey) {
		if preview, err := p.createPreview(data); err == nil {
			p.upload(ctx, p.bucketPreviews, previewKey, preview, "image/jpeg")
		}
	}

	cdnURL := p.buildCDNUrl(originalKey)

	return &kl.AdImage{
		AdID:        adID,
		Position:    position,
		OriginalURL: imgURL,
		S3Key:       originalKey,
		PreviewKey:  previewKey,
		Hash:        hash,
		Extension:   ext,
		CDNUrl:      cdnURL,
	}, nil
}

func (p *Pipeline) download(ctx context.Context, imgURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", imgURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (p *Pipeline) createPreview(data []byte) ([]byte, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	bounds := img.Bounds()
	if bounds.Dx() > p.previewWidth {
		img = imaging.Resize(img, p.previewWidth, 0, imaging.Lanczos)
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 85}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *Pipeline) upload(ctx context.Context, bucket, key string, data []byte, contentType string) error {
	_, err := p.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(contentType),
		CacheControl: aws.String("public, max-age=31536000, immutable"),
	})
	return err
}

func (p *Pipeline) objectExists(ctx context.Context, bucket, key string) bool {
	_, err := p.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err == nil
}

func (p *Pipeline) buildCDNUrl(key string) string {
	if p.cdnBase != "" {
		return p.cdnBase + "/" + key
	}
	return fmt.Sprintf("https://%s.s3.wasabisys.com/%s", p.bucketOriginals, key)
}

func getExtension(imgURL string) string {
	clean := strings.Split(imgURL, "?")[0]
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(clean), "."))
	switch ext {
	case "jpg", "jpeg", "png", "gif", "webp":
		return ext
	default:
		return "jpg"
	}
}

func init() {
	image.RegisterFormat("jpeg", "\xff\xd8", jpeg.Decode, jpeg.DecodeConfig)
	image.RegisterFormat("png", "\x89PNG", png.Decode, png.DecodeConfig)
}
