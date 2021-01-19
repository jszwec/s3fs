package s3fs_test

import (
	"bytes"
	"crypto/tls"
	"errors"
	"flag"
	"io"
	"io/fs"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

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
		bucket   = "test-github.com-jszwec-s3fs"
		testFile = "file.txt"
	)

	content := []byte("content")

	allFiles := [...]string{
		testFile,
		"dir1/file1.txt",
		"dir1/file2.txt",
		"dir1/dir11/file.txt",
		"dir2/file1.txt",
		"x/file1.txt",
		"y.txt",
		"y2.txt",
		"y3.txt",
		"z/z/file1.txt",
	}

	createBucket(t, s3cl, bucket)
	for _, f := range allFiles {
		writeFile(t, s3cl, bucket, f, content)
	}

	t.Cleanup(func() {
		out, err := s3cl.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Fatal("failed to delete bucket:", err)
		}

		for _, o := range out.Contents {
			_, err := s3cl.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    o.Key,
			})
			if err != nil {
				t.Error("failed to delete file:", err)
			}
		}
	})

	testFn := func(t *testing.T, s3fs *s3fs.FS) {
		t.Run("testing fstest", func(t *testing.T) {
			t.Parallel()
			if err := fstest.TestFS(s3fs, allFiles[:]...); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("readfile", func(t *testing.T) {
			t.Parallel()
			data, err := fs.ReadFile(s3fs, testFile)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(data, []byte("content")) {
				t.Errorf("expect: %s; got %s", data, []byte("content"))
			}
		})

		t.Run("stat", func(t *testing.T) {
			t.Parallel()

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

		t.Run("readdir", func(t *testing.T) {
			t.Parallel()

			t.Run("success", func(t *testing.T) {
				fixtures := []struct {
					desc  string
					path  string
					names []string
					modes []fs.FileMode
					isDir []bool
					size  []int
				}{
					{
						desc:  "top level",
						path:  ".",
						names: []string{"dir1", "dir2", testFile, "x", "y.txt", "y2.txt", "y3.txt", "z"},
						modes: []fs.FileMode{fs.ModeDir, fs.ModeDir, 0, fs.ModeDir, 0, 0, 0, fs.ModeDir},
						isDir: []bool{true, true, false, true, false, false, false, true},
						size:  []int{0, 0, len(content), 0, len(content), len(content), len(content), 0},
					},
					{
						desc:  "dir1",
						path:  "dir1",
						names: []string{"dir11", "file1.txt", "file2.txt"},
						modes: []fs.FileMode{fs.ModeDir, 0, 0},
						isDir: []bool{true, false, false},
						size:  []int{0, len(content), len(content)},
					},
					{
						desc:  "dir11",
						path:  "dir1/dir11",
						names: []string{"file.txt"},
						modes: []fs.FileMode{0},
						isDir: []bool{false},
						size:  []int{len(content)},
					},
				}

				for _, f := range fixtures {
					f := f
					test := func(t *testing.T, des []fs.DirEntry) {
						var (
							names []string
							modes []fs.FileMode
							isDir []bool
							size  []int
						)
						for _, de := range des {
							fi, err := de.Info()
							if err != nil {
								t.Fatal("expected nil; got ", err)
							}
							names = append(names, de.Name())
							modes = append(modes, fi.Mode())
							isDir = append(isDir, fi.IsDir())
							size = append(size, int(fi.Size()))
						}

						for _, v := range []struct {
							desc      string
							want, got interface{}
						}{
							{"names", f.names, names},
							{"modes", f.modes, modes},
							{"isDir", f.isDir, isDir},
							{"size", f.size, size},
						} {
							if !reflect.DeepEqual(v.want, v.got) {
								t.Errorf("%s: expected %v; got %v", v.desc, v.want, v.got)
							}
						}
					}

					t.Run("fs.ReadDir "+f.desc, func(t *testing.T) {
						des, err := s3fs.ReadDir(f.path)
						if err != nil {
							t.Fatalf("expected err to be nil: %v", err)
						}
						test(t, des)
					})

					t.Run("file.ReadDir "+f.desc, func(t *testing.T) {
						f, err := s3fs.Open(f.path)
						if err != nil {
							t.Fatalf("expected err to be nil: %v", err)
						}

						d, ok := f.(fs.ReadDirFile)
						if !ok {
							t.Fatal("expected file to be a directory")
						}

						des, err := d.ReadDir(-1)
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("expected err to be nil: %v", err)
						}
						test(t, des)
					})
				}
			})

			t.Run("error", func(t *testing.T) {
				fixtures := []struct {
					desc string
					path string
					err  fs.PathError
				}{
					{
						desc: "invalid path",
						path: "/",
						err:  fs.PathError{Op: "readdir", Path: "/", Err: fs.ErrInvalid},
					},
					{
						desc: "does not exist",
						path: "notexist",
						err:  fs.PathError{Op: "readdir", Path: "notexist", Err: fs.ErrNotExist},
					},
					{
						desc: "does not exist",
						path: "dir1/notexist",
						err:  fs.PathError{Op: "readdir", Path: "dir1/notexist", Err: fs.ErrNotExist},
					},
					// { TODO
					// 	desc: "readDir on a file",
					// 	path: "dir1/file1.txt",
					// 	err:  fs.PathError{Op: "readdir", Path: "dir1/file1.txt", Err: fs.ErrNotExist},
					// },
				}

				for _, f := range fixtures {
					t.Run(f.desc, func(t *testing.T) {
						_, err := s3fs.ReadDir(f.path)

						var perr *fs.PathError
						if !errors.As(err, &perr) {
							t.Fatalf("expected err to be *fs.PathError; got %[1]T: %[1]v", err)
						}

						if *perr != f.err {
							t.Errorf("want %v; got %v", f.err, perr)
						}
					})
				}
			})
		})
	}

	fixtures := []struct {
		desc string
		s3fs *s3fs.FS
	}{
		{desc: "standard", s3fs: s3fs.New(s3cl, bucket)},
		{desc: "max keys = 1", s3fs: s3fs.New(&client{MaxKeys: aws.Int64(1), S3API: s3cl}, bucket)},
		{desc: "max keys = 2", s3fs: s3fs.New(&client{MaxKeys: aws.Int64(2), S3API: s3cl}, bucket)},
		{desc: "max keys = 3", s3fs: s3fs.New(&client{MaxKeys: aws.Int64(3), S3API: s3cl}, bucket)},
	}

	for _, f := range fixtures {
		f := f
		t.Run(f.desc, func(t *testing.T) {
			t.Parallel()
			testFn(t, f.s3fs)
		})
	}
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

type client struct {
	MaxKeys *int64
	s3iface.S3API
}

func (c *client) ListObjects(in *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	if c.MaxKeys != nil {
		in.MaxKeys = c.MaxKeys
	}
	return c.S3API.ListObjects(in)
}
