package s3fs_test

import (
	"bytes"
	"crypto/tls"
	"errors"
	"flag"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/jszwec/s3fs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	endpoint = flag.String("endpoint", "localhost:4566", "s3 endpoint")
)

var (
	accessKeyID = envDefault("AWS_ACCESS_KEY_ID", "1")
	secretKey   = envDefault("AWS_SECRET_ACCESS_KEY", "1")
	region      = envDefault("AWS_REGION", "us-east-1")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestFS(t *testing.T) {
	s3cl := newClient(t)

	const (
		bucket   = "test"
		testFile = "text.txt"
	)

	content := []byte("content")

	createBucket(t, s3cl, bucket)
	writeFile(t, s3cl, bucket, testFile, content)

	s3fs := s3fs.New(s3cl, bucket)

	t.Run("readfile", func(t *testing.T) {
		data, err := fs.ReadFile(s3fs, testFile)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(data, []byte("content")) {
			t.Errorf("expect: %s; got %s", data, []byte("content"))
		}
	})

	t.Run("stat", func(t *testing.T) {
		test := func(t *testing.T, fi fs.FileInfo) {
			t.Helper()

			if fi.IsDir() {
				t.Error("expected false")
			}

			if fi.Mode() != 0 {
				t.Errorf("want %d; got %d", 0, fi.Mode())
			}

			if fi.Sys() != nil {
				t.Error("expected Sys to be nil")
			}
		}

		t.Run("file stat", func(t *testing.T) {
			f, err := s3fs.Open(testFile)
			if err != nil {
				t.Fatal("expected err to be nil")
			}
			defer f.Close()

			fi, err := f.Stat()
			if err != nil {
				t.Fatal("expected err to be nil")
			}

			test(t, fi)
		})

		t.Run("fs stat", func(t *testing.T) {
			fi, err := s3fs.Stat(testFile)
			if err != nil {
				t.Fatal("expected err to be nil")
			}

			test(t, fi)
		})

		t.Run("does not exist", func(t *testing.T) {
			_, err := s3fs.Stat("not-existing")
			var pathErr *fs.PathError
			if !errors.As(err, &pathErr) {
				t.Fatal("expected err to be *PathError")
			}
		})
	})
}

func newClient(t *testing.T) s3iface.S3API {
	t.Helper()

	cl := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	s, err := session.NewSession(
		aws.NewConfig().
			WithEndpoint(*endpoint).
			WithRegion(region).
			WithS3ForcePathStyle(true).
			WithHTTPClient(cl).
			WithCredentials(credentials.NewStaticCredentials(accessKeyID, secretKey, "")),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s3.New(s)
}

func writeFile(t *testing.T, cl s3iface.S3API, bucket, name string, data []byte) {
	t.Helper()

	uploader := s3manager.NewUploaderWithClient(cl)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   strings.NewReader("content"),
		Bucket: &bucket,
		Key:    &name,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func createBucket(t *testing.T, cl s3iface.S3API, bucket string) {
	t.Helper()

	_, err := cl.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func envDefault(env, def string) string {
	if os.Getenv(env) == "" {
		return def
	}
	return env
}
