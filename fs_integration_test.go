package s3fs_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jszwec/s3fs"
)

var (
	endpoint   = flag.String("endpoint", "http://localhost:4566", "s3 endpoint")
	bucket     = flag.String("bucket", "test-github.com-jszwec-s3fs", "bucket name")
	skipVerify = flag.Bool("skip-verify", true, "http insecure skip verify")
)

var (
	accessKeyID = envDefault("S3FS_TEST_AWS_ACCESS_KEY_ID", "1")
	secretKey   = envDefault("S3FS_TEST_AWS_SECRET_ACCESS_KEY", "1")
	region      = envDefault("S3FS_TEST_AWS_REGION", "us-east-1")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestFS(t *testing.T) {
	s3cl := newClient(t)

	const testFile = "file.txt"

	content := []byte("content")

	allFiles := [...]string{
		testFile,
		"dir/a.txt",
		"dir1/file1.txt",
		"dir1/file2.txt",
		"dir1/dir11/file.txt",
		"dir2/file1.txt",
		"x/file1.txt",
		"y.txt",
		"y2.txt",
		"y3.txt",
		"z/z/file1.txt",
		"a.txt",
		"a/b.txt",
	}

	createBucket(t, s3cl, *bucket)
	cleanBucket(t, s3cl, *bucket)

	t.Run("list empty bucket", func(t *testing.T) {
		fi, err := s3fs.New(context.TODO(), s3cl, *bucket).Open(".")
		if err != nil {
			t.Errorf("want err to be nil; got %v", err)
		}

		dir := fi.(fs.ReadDirFile)
		fixtures := []struct {
			desc string
			n    int
			err  error
		}{
			{"n > 0", 1, io.EOF},
			{"n <= 0", -1, nil},
		}

		for _, f := range fixtures {
			f := f
			t.Run(f.desc, func(t *testing.T) {
				des, err := dir.ReadDir(f.n)
				if err != f.err {
					t.Errorf("want err to be %v; got %v", f.err, err)
				}

				if des == nil {
					t.Error("want des to not be a nil slice")
				}

				if len(des) > 0 {
					t.Errorf("expected the directory to be empty; got %d elements", len(des))
				}
			})
		}
	})

	for _, f := range allFiles {
		writeFile(t, s3cl, *bucket, f)
	}

	t.Cleanup(func() {
		cleanBucket(t, s3cl, *bucket)

		t.Log("test stats:")
		t.Log("ListObjects calls:", atomic.LoadInt64(&listC))
		t.Log("GetObject calls:", atomic.LoadInt64(&getC))
	})

	testFn := func(t *testing.T, s3fs *s3fs.S3FS) {
		t.Run("testing fstest", func(t *testing.T) {
			if testing.Short() {
				t.Skip("short test enabled")
			}

			t.Parallel()
			if err := fstest.TestFS(s3fs, allFiles[:]...); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("readfile", func(t *testing.T) {
			t.Parallel()

			t.Run("success", func(t *testing.T) {
				data, err := fs.ReadFile(s3fs, testFile)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(data, []byte("content")) {
					t.Errorf("expect: %s; got %s", data, []byte("content"))
				}
			})

			t.Run("error", func(t *testing.T) {
				t.Run("invalid path", func(t *testing.T) {
					_, err := fs.ReadFile(s3fs, "/")
					if err == nil {
						t.Fatal("expected error")
					}

					var pathErr *fs.PathError
					if !errors.As(err, &pathErr) {
						t.Fatal("expected err to be *PathError")
					}

					expected := fs.PathError{
						Op:   "open",
						Path: "/",
						Err:  fs.ErrInvalid,
					}
					if *pathErr != expected {
						t.Fatalf("want %v; got %v", expected, *pathErr)
					}
				})

				t.Run("directory", func(t *testing.T) {
					_, err := fs.ReadFile(s3fs, ".")
					if err == nil {
						t.Fatal("expected error")
					}

					var perr *fs.PathError
					if !errors.As(err, &perr) {
						t.Fatal("expected err to be *PathError")
					}

					if perr.Op != "read" {
						t.Errorf("want %v; got %v", "read", perr.Op)
					}

					if perr.Path != "." {
						t.Errorf("want %v; got %v", ".", perr.Path)
					}

					if perr.Err.Error() != "is a directory" {
						t.Errorf("want %v; got %v", "is a directory", perr.Err.Error())
					}
				})
			})
		})

		t.Run("stat file", func(t *testing.T) {
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
				defer func() { _ = f.Close() }()

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

			t.Run("invalid path", func(t *testing.T) {
				_, err := s3fs.Stat("/")
				var pathErr *fs.PathError
				if !errors.As(err, &pathErr) {
					t.Fatal("expected err to be *PathError")
				}

				expected := fs.PathError{
					Op:   "stat",
					Path: "/",
					Err:  fs.ErrInvalid,
				}
				if *pathErr != expected {
					t.Fatalf("want %v; got %v", expected, *pathErr)
				}
			})

			t.Run("does not exist", func(t *testing.T) {
				_, err := s3fs.Stat("not-existing")
				var pathErr *fs.PathError
				if !errors.As(err, &pathErr) {
					t.Fatal("expected err to be *PathError")
				}

				expected := fs.PathError{
					Op:   "stat",
					Path: "not-existing",
					Err:  fs.ErrNotExist,
				}
				if *pathErr != expected {
					t.Fatalf("want %v; got %v", expected, *pathErr)
				}
			})
		})

		t.Run("stat dir", func(t *testing.T) {
			t.Parallel()

			test := func(t *testing.T, fi fs.FileInfo) {
				t.Helper()

				if !fi.IsDir() {
					t.Error("expected true")
				}

				if fi.Mode() != fs.ModeDir {
					t.Errorf("want %d; got %d", fs.ModeDir, fi.Mode())
				}

				if fi.Sys() != nil {
					t.Error("expected Sys to be nil")
				}
			}

			t.Run("top level", func(t *testing.T) {
				fi, err := s3fs.Stat(".")
				if err != nil {
					t.Fatal("expected err to be nil")
				}
				test(t, fi)

				if fi.Name() != "." {
					t.Errorf("want name=%q; got %q", ".", fi.Name())
				}
			})

			t.Run("open z", func(t *testing.T) {
				fi, err := s3fs.Stat("z")
				if err != nil {
					t.Fatal("expected err to be nil")
				}
				test(t, fi)
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
						names: []string{"a", "a.txt", "dir", "dir1", "dir2", testFile, "x", "y.txt", "y2.txt", "y3.txt", "z"},
						modes: []fs.FileMode{fs.ModeDir, 0, fs.ModeDir, fs.ModeDir, fs.ModeDir, 0, fs.ModeDir, 0, 0, 0, fs.ModeDir},
						isDir: []bool{true, false, true, true, true, false, true, false, false, false, true},
						size:  []int{0, len(content), 0, 0, 0, len(content), 0, len(content), len(content), len(content), 0},
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
					{
						desc: "readDir on a file",
						path: "dir1/file1.txt",
						err:  fs.PathError{Op: "readdir", Path: "dir1/file1.txt", Err: errors.New("not a dir")},
					},
				}

				for _, f := range fixtures {
					t.Run(f.desc, func(t *testing.T) {
						_, err := s3fs.ReadDir(f.path)

						var perr *fs.PathError
						if !errors.As(err, &perr) {
							t.Fatalf("expected err to be *fs.PathError; got %[1]T: %[1]v", err)
						}

						if perr.Op != f.err.Op {
							t.Errorf("want %v; got %v", f.err.Op, perr.Op)
						}

						if perr.Path != f.err.Path {
							t.Errorf("want %v; got %v", f.err.Path, perr.Path)
						}

						if perr.Err.Error() != f.err.Err.Error() {
							t.Errorf("want %v; got %v", f.err.Err.Error(), perr.Err.Error())
						}
					})
				}
			})
		})

		t.Run("subfs", func(t *testing.T) {
			t.Run("existing", func(t *testing.T) {
				fsys, err := fs.Sub(s3fs, "dir1/dir11")
				if err != nil {
					t.Fatal(err)
				}

				t.Run("fs.Stat", func(t *testing.T) {
					fi, err := fs.Stat(fsys, "file.txt")
					if err != nil {
						t.Fatal(err)
					}
					if fi.Name() != "file.txt" {
						t.Errorf("expected file.txt got %s", fi.Name())
					}

					t.Run("not exist", func(t *testing.T) {
						_, err = fs.Stat(fsys, "not-exist")
						var perr *fs.PathError
						if !errors.As(err, &perr) {
							t.Fatalf("expected err to be PathError: got %#v", err)
						}

						// currently we don't implement fs.SubFS.
						// fs.Sub calls open instead of Stat.
						if perr.Op != "open" {
							t.Errorf("expected op to be open; got %s", perr.Op)
						}
					})
				})

				t.Run("fs.ReadDir", func(t *testing.T) {
					files, err := fs.ReadDir(fsys, ".")
					if err != nil {
						t.Fatal(err)
					}

					if len(files) != 1 {
						t.Fatalf("expected 1 file in dir1/dir11; got %d", len(files))
					}
					if files[0].Name() != "file.txt" {
						t.Errorf("expected file to be file.txt; got %s", files[0].Name())
					}

					t.Run("not exist", func(t *testing.T) {
						_, err := fs.ReadDir(fsys, "not-exist")
						var perr *fs.PathError
						if !errors.As(err, &perr) {
							t.Fatalf("expected err to be PathError: got %#v", err)
						}

						if perr.Op != "readdir" {
							t.Errorf("expected op to be readdir; got %s", perr.Op)
						}
					})
				})

				t.Run("open", func(t *testing.T) {
					f, err := fsys.Open(".")
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = f.Close() }()

					dir, ok := f.(fs.ReadDirFile)
					if !ok {
						t.Fatal("expected file to be a directory")
					}

					fi, err := dir.Stat()
					if err != nil {
						t.Fatal(err)
					}
					if fi.Name() != "dir11" {
						t.Errorf("expected dir name to bedir11; got %s", fi.Name())
					}

					files, err := dir.ReadDir(-1)
					if err != nil {
						t.Fatal(err)
					}

					if len(files) != 1 {
						t.Fatalf("expected 1 file in dir1/dir11; got %d", len(files))
					}
					if files[0].Name() != "file.txt" {
						t.Errorf("expected file to be file.txt; got %s", files[0].Name())
					}
				})
			})
		})
	}

	fixtures := []struct {
		desc string
		s3fs *s3fs.S3FS
	}{
		{desc: "standard", s3fs: s3fs.New(context.TODO(), s3cl, *bucket)},
		{desc: "max keys = 1", s3fs: s3fs.New(context.TODO(), s3cl, *bucket)},
		{desc: "max keys = 2", s3fs: s3fs.New(context.TODO(), s3cl, *bucket)},
		{desc: "max keys = 3", s3fs: s3fs.New(context.TODO(), s3cl, *bucket)},
	}

	for _, f := range fixtures {
		f := f
		t.Run(f.desc, func(t *testing.T) {
			t.Parallel()
			testFn(t, f.s3fs)
		})
	}
}

func newClient(t *testing.T) s3fs.S3Api {
	t.Helper()

	credProvider := credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, "")
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           *endpoint,
			SigningRegion: region,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credProvider),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		t.Fatal(err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = *skipVerify
	})
	return &modTimeTruncateClient{&metricClient{s3Client}}
}

func createBucket(t *testing.T, s3Api s3fs.S3Api, bucket string) {
	t.Helper()

	_, err := s3Api.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		if errors.As(err, &alreadyOwned) {
			return
		}
		t.Fatal(err)
	}
}

func cleanBucket(t *testing.T, s3Api s3fs.S3Api, bucket string) {
	t.Helper()

	out, err := s3Api.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatal("failed to delete bucket:", err)
	}

	for _, o := range out.Contents {
		_, err := s3Api.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    o.Key,
		})
		if err != nil {
			t.Error("failed to delete file:", err)
		}
	}
}

func envDefault(env, def string) string {
	if os.Getenv(env) == "" {
		return def
	}
	return os.Getenv(env)
}

type modTimeTruncateClient struct {
	s3fs.S3Api
}

// Minio returns modTime that includes microseconds if data comes from ListObjects
// while data coming from GetObject's modTimes are accurate down to seconds.
// To make this test pass while using Minio we build this client that truncates
// modTimes to Second.
func (c *modTimeTruncateClient) ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	out, err := c.S3Api.ListObjects(ctx, params, optFns...)
	if err != nil {
		return out, err
	}

	for _, o := range out.Contents {
		o.LastModified = aws.Time(o.LastModified.Truncate(time.Second))
	}
	return out, err
}

var (
	// global metrics for this test.
	listC int64
	getC  int64
)

type metricClient struct {
	s3fs.S3Api
}

func (c *metricClient) ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	atomic.AddInt64(&listC, 1)
	return c.S3Api.ListObjects(ctx, params, optFns...)
}

func writeFile(t *testing.T, s3Api s3fs.S3Api, bucket, name string) {
	t.Helper()

	_, err := s3Api.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(name),
		Body:   strings.NewReader("content"),
	})
	if err != nil {
		t.Fatal(err)
	}
}
