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
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jszwec/s3fs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	endpoint   = flag.String("endpoint", "localhost:4566", "s3 endpoint")
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

func TestSeeker(t *testing.T) {
	s3cl := newClient(t)

	const testFile = "file.txt"
	content := []byte("content")

	createBucket(t, s3cl, *bucket)
	cleanBucket(t, s3cl, *bucket)

	writeFile(t, s3cl, *bucket, testFile, content)

	t.Cleanup(func() {
		cleanBucket(t, s3cl, *bucket)

		t.Log("test stats:")
		t.Log("ListObjects calls:", atomic.LoadInt64(&listC))
		t.Log("GetObject calls:", atomic.LoadInt64(&getC))
	})

	t.Run("'s3fs.New' does not implement Seeker", func(t *testing.T) {
		testFs := s3fs.New(s3cl, *bucket)
		data, err := testFs.Open(testFile)

		if err != nil {
			t.Fatal(err)
		}

		_, ok := data.(io.Seeker)

		if ok {
			t.Fatalf("Expected 'data' to not implement the Seeker interface")
		}
	})

	t.Run("seek throws error if file changed", func(t *testing.T) {
		const otherTestFile = "otherFile.txt"
		originalContent := []byte("con")
		changedContent := []byte("tent")

		writeFile(t, s3cl, *bucket, otherTestFile, originalContent)

		testFs := s3fs.NewSeekable(s3cl, *bucket)
		data, err := testFs.Open(otherTestFile)

		if err != nil {
			t.Fatal(err)
		}

		if _, err := data.(io.Seeker).Seek(0, io.SeekEnd); err != nil {
			t.Fatal(err)
		}

		deleteFile(t, s3cl, *bucket, otherTestFile)
		writeFile(t, s3cl, *bucket, otherTestFile, changedContent)

		_, err = data.(io.Seeker).Seek(0, io.SeekStart)

		if err == nil {
			t.Fatalf("Expected error, got nil")
		}

		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
	})

	t.Run("seek once", func(t *testing.T) {
		fixtures := []struct {
			desc     string
			offset   int64
			whence   int
			expected int64
		}{
			{
				desc:     "whence SeekStart ",
				offset:   2,
				whence:   io.SeekStart,
				expected: 2,
			},
			{
				desc:     "whence SeekCurrent",
				offset:   4,
				whence:   io.SeekCurrent,
				expected: 4,
			},
			{
				desc:     "whence SeekEnd",
				offset:   -1,
				whence:   io.SeekEnd,
				expected: int64(len(content)) - 1,
			},
		}

		for _, f := range fixtures {
			f := f
			t.Run(f.desc, func(t *testing.T) {
				testFs := s3fs.NewSeekable(s3cl, *bucket)
				data, err := testFs.Open(testFile)
				if err != nil {
					t.Fatal(err)
				}

				actual, err := data.(io.Seeker).Seek(f.offset, f.whence)
				if err != nil {
					t.Fatal(err)
				}

				if actual != f.expected {
					t.Fatalf("Expected %d, got %d", f.expected, actual)
				}

			})
		}
	})

	t.Run("seek with errors", func(t *testing.T) {
		fixtures := []struct {
			desc         string
			offset       int64
			whence       int
			errorMessage string
		}{
			{
				desc:         "seek before beginning with whence SeekCurrent",
				offset:       -1,
				whence:       io.SeekCurrent,
				errorMessage: "s3fs.file.Seek: seeked to a negative position",
			},
			{
				desc:         "seek before beginning with whence SeekStart",
				offset:       -1,
				whence:       io.SeekStart,
				errorMessage: "s3fs.file.Seek: seeked to a negative position",
			},
			{
				desc:         "seek with invalid whence",
				offset:       0,
				whence:       3,
				errorMessage: "s3fs.file.Seek: invalid whence",
			},
		}

		for _, f := range fixtures {
			f := f
			t.Run(f.desc, func(t *testing.T) {
				testFs := s3fs.NewSeekable(s3cl, *bucket)
				data, err := testFs.Open(testFile)
				if err != nil {
					t.Fatal(err)
				}

				_, err = data.(io.Seeker).Seek(f.offset, f.whence)
				if err == nil {
					t.Fatalf("Expected error after seeking to invalid position, got nil")
				}
				if err.Error() != f.errorMessage {
					t.Fatalf("Expected %s, got %v", f.errorMessage, err)
				}
			})
		}
	})

	t.Run("seek from other starting position", func(t *testing.T) {
		fixtures := []struct {
			desc          string
			initialOffset int
			offset        int64
			whence        int
			expected      int64
		}{
			{
				desc:          "whence SeekStart",
				initialOffset: 3,
				offset:        2,
				whence:        io.SeekStart,
				expected:      2,
			},
			{
				desc:          "whence SeekCurrent",
				initialOffset: 3,
				offset:        3,
				whence:        io.SeekCurrent,
				expected:      6,
			},
			{
				desc:          "whence SeekEnd",
				initialOffset: 3,
				offset:        -1,
				whence:        io.SeekEnd,
				expected:      int64(len(content)) - 1,
			},
		}

		for _, f := range fixtures {
			f := f
			t.Run(f.desc, func(t *testing.T) {
				testFs := s3fs.NewSeekable(s3cl, *bucket)
				data, err := testFs.Open(testFile)
				if err != nil {
					t.Fatal(err)
				}

				readBuffer := make([]byte, f.initialOffset)
				readBytes, err := data.Read(readBuffer)
				if err != nil {
					t.Fatal(err)
				}
				if readBytes != f.initialOffset {
					t.Fatalf("Read failed during test setup")
				}

				actual, err := data.(io.Seeker).Seek(f.offset, f.whence)
				if err != nil {
					t.Fatal(err)
				}

				if actual != f.expected {
					t.Fatalf("Expected %d, got %d", f.expected, actual)
				}
			})
		}
	})

	t.Run("seek then read", func(t *testing.T) {
		fixtures := []struct {
			desc         string
			readBytes    int
			offset       int64
			whence       int
			expected     []byte
			expectingEOF bool
		}{
			{
				desc:         "whence SeekStart",
				readBytes:    3,
				offset:       2,
				whence:       io.SeekStart,
				expected:     content[2:5],
				expectingEOF: false,
			},
			{
				desc:         "whence SeekCurrent",
				readBytes:    1,
				offset:       1,
				whence:       io.SeekCurrent,
				expected:     []byte("o"),
				expectingEOF: false,
			},
			{
				desc:         "seek to end then read 0",
				readBytes:    0,
				offset:       0,
				whence:       io.SeekEnd,
				expected:     []byte(""),
				expectingEOF: true,
			},
			{
				desc:         "whence SeekStart with EOF",
				readBytes:    2,
				offset:       5,
				whence:       io.SeekStart,
				expected:     content[5:7],
				expectingEOF: true,
			},
			{
				desc:         "whence SeekCurrent with EOF",
				readBytes:    3,
				offset:       4,
				whence:       io.SeekCurrent,
				expected:     content[4:7],
				expectingEOF: true,
			},
			{
				desc:         "whence SeekEnd with EOF",
				readBytes:    3,
				offset:       -3,
				whence:       io.SeekEnd,
				expected:     content[len(content)-3:],
				expectingEOF: true,
			},
			{
				desc:         "zero offset and read more than fits the buffer",
				readBytes:    100,
				offset:       0,
				whence:       io.SeekStart,
				expected:     []byte("content"),
				expectingEOF: true,
			},
			{
				desc:         "whence SeekStart offset and read more than fits the buffer",
				readBytes:    100,
				offset:       1,
				whence:       io.SeekStart,
				expected:     []byte("ontent"),
				expectingEOF: true,
			},
			{
				desc:         "whence SeekCurrent offset and read more than fits the buffer",
				readBytes:    100,
				offset:       1,
				whence:       io.SeekCurrent,
				expected:     []byte("ontent"),
				expectingEOF: true,
			},
			{
				desc:         "whence SeekEnd to the end of the file and then read",
				readBytes:    10,
				offset:       0,
				whence:       io.SeekEnd,
				expected:     []byte(""),
				expectingEOF: true,
			},
			{
				desc:         "whence SeekEnd past the end of the file and then read",
				readBytes:    10,
				offset:       1,
				whence:       io.SeekEnd,
				expected:     []byte(""),
				expectingEOF: true,
			},
		}

		for _, f := range fixtures {
			f := f
			t.Run(f.desc, func(t *testing.T) {
				testFs := s3fs.NewSeekable(s3cl, *bucket)
				data, err := testFs.Open(testFile)
				if err != nil {
					t.Fatal(err)
				}

				readSeekers := []struct {
					desc   string
					seeker io.ReadSeeker
				}{
					{desc: "file", seeker: data.(io.ReadSeeker)},
					{desc: "bytes reader", seeker: bytes.NewReader(content)},
				}

				for _, rs := range readSeekers {
					rs := rs
					t.Run(rs.desc, func(t *testing.T) {
						_, err = rs.seeker.Seek(f.offset, f.whence)
						if err != nil {
							t.Fatal(err)
						}

						var buf bytes.Buffer
						_, err := io.CopyN(&buf, rs.seeker, int64(f.readBytes))
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatal(err)
						}

						if buf.String() != string(f.expected) {
							t.Errorf("expected %s, got %s", f.expected, buf.String())
						}
						if f.expectingEOF {
							newlyReadBytes, err := rs.seeker.Read(make([]byte, 0))
							if newlyReadBytes != 0 {
								t.Fatalf("Read returned unexpected number of bytes: expected 0, got %d", newlyReadBytes)
							}
							if err == nil {
								t.Fatalf("Expected io.EOF error, got nil")
							}
							if !errors.Is(err, io.EOF) {
								t.Fatal(err)
							}
						}
					})
				}
			})
		}
	})

	t.Run("seek twice then read", func(t *testing.T) {
		fixtures := []struct {
			desc         string
			readBytes    int
			firstOffset  int64
			firstWhence  int
			secondOffset int64
			expected     []byte
			expectingEOF bool
		}{
			{
				desc:         "whence SeekStart",
				readBytes:    2,
				firstOffset:  1,
				firstWhence:  io.SeekStart,
				secondOffset: 2,
				expected:     content[3:5],
				expectingEOF: false,
			},
			{
				desc:         "whence SeekCurrent",
				readBytes:    1,
				firstOffset:  2,
				firstWhence:  io.SeekCurrent,
				secondOffset: 3,
				expected:     content[5:6],
				expectingEOF: false,
			},
			{
				desc:         "whence SeekEnd",
				readBytes:    2,
				firstOffset:  -4,
				firstWhence:  io.SeekEnd,
				secondOffset: 1,
				expected:     content[4:6],
				expectingEOF: false,
			},
			{
				desc:         "whence SeekStart with EOF",
				readBytes:    5,
				firstOffset:  1,
				firstWhence:  io.SeekStart,
				secondOffset: 2,
				expected:     content[3:],
				expectingEOF: true,
			},
			{
				desc:         "whence SeekCurrent with EOF",
				readBytes:    2,
				firstOffset:  2,
				firstWhence:  io.SeekCurrent,
				secondOffset: 3,
				expected:     content[5:],
				expectingEOF: true,
			},
			{
				desc:         "whence SeekEnd with EOF",
				readBytes:    7,
				firstOffset:  -5,
				firstWhence:  io.SeekEnd,
				secondOffset: 1,
				expected:     content[3:],
				expectingEOF: true,
			},
		}

		for _, f := range fixtures {
			f := f
			t.Run(f.desc, func(t *testing.T) {
				testFs := s3fs.NewSeekable(s3cl, *bucket)
				data, err := testFs.Open(testFile)
				if err != nil {
					t.Fatal(err)
				}

				readSeekers := []struct {
					desc   string
					seeker io.ReadSeeker
				}{
					{desc: "file", seeker: data.(io.ReadSeeker)},
					{desc: "bytes reader", seeker: bytes.NewReader(content)},
				}

				for _, rs := range readSeekers {
					rs := rs
					t.Run(rs.desc, func(t *testing.T) {
						_, err = rs.seeker.Seek(f.firstOffset, f.firstWhence)
						if err != nil {
							t.Fatal(err)
						}

						_, err = rs.seeker.Seek(f.secondOffset, io.SeekCurrent)
						if err != nil {
							t.Fatal(err)
						}

						var buf bytes.Buffer
						_, err := io.CopyN(&buf, rs.seeker, int64(f.readBytes))
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatal(err)
						}

						if buf.String() != string(f.expected) {
							t.Errorf("expected %s, got %s", f.expected, buf.String())
						}
						if f.expectingEOF {
							newlyReadBytes, err := rs.seeker.Read(make([]byte, 0))
							if newlyReadBytes != 0 {
								t.Fatalf("Read returned unexpected number of bytes: expected 0, got %d", newlyReadBytes)
							}
							if err == nil {
								t.Fatalf("Expected io.EOF error, got nil")
							}
							if !errors.Is(err, io.EOF) {
								t.Fatal(err)
							}
						}
					})
				}
			})
		}
	})
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
		fi, err := s3fs.New(s3cl, *bucket).Open(".")
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
		writeFile(t, s3cl, *bucket, f, content)
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
					defer f.Close()

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
		{desc: "standard", s3fs: s3fs.New(s3cl, *bucket)},
		{desc: "max keys = 1", s3fs: s3fs.New(&client{MaxKeys: aws.Int64(1), S3API: s3cl}, *bucket)},
		{desc: "max keys = 2", s3fs: s3fs.New(&client{MaxKeys: aws.Int64(2), S3API: s3cl}, *bucket)},
		{desc: "max keys = 3", s3fs: s3fs.New(&client{MaxKeys: aws.Int64(3), S3API: s3cl}, *bucket)},
	}

	for _, f := range fixtures {
		f := f
		t.Run(f.desc, func(t *testing.T) {
			t.Parallel()
			testFn(t, f.s3fs)
		})
	}
}

func TestDirRead(t *testing.T) {
	type fileinfo struct {
		name  string
		isDir bool
	}

	tests := []struct {
		desc     string
		n        int
		outs     []s3.ListObjectsOutput
		expected [][]fileinfo
	}{
		{
			desc: "all in one request - dir first",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c", "e"}, []string{"b", "d", "f"}),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"b", false}},
				{{"c", true}},
				{{"d", false}},
				{{"e", true}},
				{{"f", false}},
			},
		},
		{
			desc: "all in one request - n = 0",
			n:    0,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c", "e"}, []string{"b", "d", "f"}),
			},
			expected: [][]fileinfo{
				{
					{"a", true},
					{"b", false},
					{"c", true},
					{"d", false},
					{"e", true},
					{"f", false},
				},
			},
		},
		{
			desc: "all in one request - n = 2",
			n:    2,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a"}, nil),
				newListOutput([]string{"c"}, []string{"b", "d"}),
				newListOutput([]string{"e"}, nil),
				newListOutput(nil, []string{"f"}),
			},
			expected: [][]fileinfo{
				{
					{"a", true},
					{"b", false},
				},
				{
					{"c", true},
					{"d", false},
				},
				{
					{"e", true},
					{"f", false},
				},
			},
		},
		{
			desc: "one per request - dir first",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a"}, nil),
				newListOutput(nil, []string{"b"}),
				newListOutput([]string{"c"}, []string{"d"}),
				newListOutput([]string{"e"}, nil),
				newListOutput(nil, []string{"f"}),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"b", false}},
				{{"c", true}},
				{{"d", false}},
				{{"e", true}},
				{{"f", false}},
			},
		},
		{
			desc: "all in one request - file first",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"b", "d", "f"}, []string{"a", "c", "e"}),
			},
			expected: [][]fileinfo{
				{{"a", false}},
				{{"b", true}},
				{{"c", false}},
				{{"d", true}},
				{{"e", false}},
				{{"f", true}},
			},
		},
		{
			desc: "with dir duplicates",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c"}, []string{"b"}),
				newListOutput([]string{"c", "e", "c"}, []string{"d"}),
				newListOutput([]string{"e", "a"}, []string{"f"}),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"b", false}},
				{{"c", true}},
				{{"d", false}},
				{{"e", true}},
				{{"f", false}},
			},
		},

		{
			desc: "all in one request - dirs only",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c", "e"}, nil),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"c", true}},
				{{"e", true}},
			},
		},
		{
			desc: "single dir per request - dirs only",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a"}, nil),
				newListOutput([]string{"c"}, nil),
				newListOutput([]string{"e"}, nil),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"c", true}},
				{{"e", true}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			f, err := s3fs.New(&mockClient{
				outs: test.outs,
			}, "test").Open(".")
			if err != nil {
				t.Fatal("expected err to be nil; got ", err)
			}

			fi, err := f.Stat()
			if err != nil {
				t.Fatal("expected err to be nil; got ", err)
			}

			if !fi.IsDir() {
				t.Fatal("expected the file to be a directory")
			}

			var fis [][]fileinfo
			for {
				files, err := f.(fs.ReadDirFile).ReadDir(test.n)
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatal("did not expect err:", err)
				}

				if len(files) > 0 {
					var out []fileinfo
					for _, f := range files {
						out = append(out, fileinfo{f.Name(), f.IsDir()})
					}
					fis = append(fis, out)
				}

				if test.n <= 0 || errors.Is(err, io.EOF) {
					break
				}
			}

			if !reflect.DeepEqual(fis, test.expected) {
				t.Errorf("want %v; got %v", test.expected, fis)
			}
		})
	}
}

type mockClient struct {
	s3iface.S3API
	outs []s3.ListObjectsOutput
	i    int
}

func (c *mockClient) ListObjects(in *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	defer func() { c.i++ }()
	if c.i < len(c.outs) {
		return &c.outs[c.i], nil
	}

	return &s3.ListObjectsOutput{
		IsTruncated: aws.Bool(false),
	}, nil
}

func newListOutput(dirs, files []string) (out s3.ListObjectsOutput) {
	for _, d := range dirs {
		out.CommonPrefixes = append(out.CommonPrefixes, &s3.CommonPrefix{
			Prefix: aws.String(d),
		})
	}

	for _, f := range files {
		out.Contents = append(out.Contents, &s3.Object{
			Key:          aws.String(f),
			Size:         aws.Int64(0),
			LastModified: aws.Time(time.Time{}),
		})
	}
	return out
}

func newClient(t *testing.T) s3iface.S3API {
	t.Helper()

	cl := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: *skipVerify,
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

	return &modTimeTruncateClient{&metricClient{s3.New(s)}}
}

func writeFile(t *testing.T, cl s3iface.S3API, bucket, name string, data []byte) {
	t.Helper()

	uploader := s3manager.NewUploaderWithClient(cl)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   strings.NewReader(string(data)),
		Bucket: &bucket,
		Key:    &name,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func deleteFile(t *testing.T, cl s3iface.S3API, bucket, name string) {
	t.Helper()

	_, err := cl.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
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
		if awserr, ok := err.(awserr.Error); ok && awserr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou {
			return
		}
		t.Fatal(err)
	}
}

func cleanBucket(t *testing.T, cl s3iface.S3API, bucket string) {
	t.Helper()

	out, err := cl.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatal("failed to delete bucket:", err)
	}

	for _, o := range out.Contents {
		_, err := cl.DeleteObject(&s3.DeleteObjectInput{
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

type modTimeTruncateClient struct {
	s3iface.S3API
}

// Minio returns modTime that includes microseconds if data comes from ListObjects
// while data coming from GetObject's modTimes are accurate down to seconds.
// To make this test pass while using Minio we build this client that truncates
// modTimes to Second.
func (c *modTimeTruncateClient) ListObjects(in *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	out, err := c.S3API.ListObjects(in)
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
	s3iface.S3API
}

func (c *metricClient) ListObjects(in *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	atomic.AddInt64(&listC, 1)
	return c.S3API.ListObjects(in)
}

func (c *metricClient) GetObject(in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	atomic.AddInt64(&getC, 1)
	return c.S3API.GetObject(in)
}
