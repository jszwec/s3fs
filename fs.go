// Package s3fs provides a S3 implementation for Go1.16 filesystem interface.
//
package s3fs

import (
	"errors"
	"io/fs"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	_ fs.FS        = (*S3FS)(nil)
	_ fs.StatFS    = (*S3FS)(nil)
	_ fs.ReadDirFS = (*S3FS)(nil)
)

var errNotDir = errors.New("not a dir")

// S3FS is a S3 filesystem implementation.
//
// S3 has a flat structure instead of a hierarchy. S3FS simulates directories
// by using prefixes and delims ("/"). Because directories are simulated, ModTime
// is always a default Time value (IsZero returns true).
type S3FS struct {
	cl     s3iface.S3API
	bucket string
}

// New returns a new filesystem that works on the specified bucket.
func New(cl s3iface.S3API, bucket string) *S3FS {
	return &S3FS{
		cl:     cl,
		bucket: bucket,
	}
}

// Open implements fs.FS.
func (f *S3FS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	if name == "." {
		return openDir(f.cl, f.bucket, name)
	}

	out, err := f.cl.GetObject(&s3.GetObjectInput{
		Key:    &name,
		Bucket: &f.bucket,
	})

	if err != nil {
		if isNotFoundErr(err) {
			switch d, err := openDir(f.cl, f.bucket, name); {
			case err == nil:
				return d, nil
			case !isNotFoundErr(err) && !errors.Is(err, errNotDir) && !errors.Is(err, fs.ErrNotExist):
				return nil, err
			}

			return nil, &fs.PathError{
				Op:   "open",
				Path: name,
				Err:  fs.ErrNotExist,
			}
		}

		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  err,
		}
	}

	statFunc := func() (fs.FileInfo, error) {
		return stat(f.cl, f.bucket, name)
	}

	if out.ContentLength != nil && out.LastModified != nil {
		// if we got all the information from GetObjectOutput
		// then we can cache fileinfo instead of making
		// another call in case Stat is called.
		statFunc = func() (fs.FileInfo, error) {
			return &fileInfo{
				name:    path.Base(name),
				size:    *out.ContentLength,
				modTime: *out.LastModified,
			}, nil
		}
	}

	return &file{
		ReadCloser: out.Body,
		stat:       statFunc,
	}, nil
}

// Stat implements fs.StatFS.
func (f *S3FS) Stat(name string) (fs.FileInfo, error) {
	fi, err := stat(f.cl, f.bucket, name)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}
	return fi, nil
}

// ReadDir implements fs.ReadDirFS.
func (f *S3FS) ReadDir(name string) ([]fs.DirEntry, error) {
	d, err := openDir(f.cl, f.bucket, name)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  err,
		}
	}
	return d.ReadDir(-1)
}

func stat(s3cl s3iface.S3API, bucket, name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}

	if name == "." {
		return &dir{
			s3cl:   s3cl,
			bucket: bucket,
			fileInfo: fileInfo{
				name: ".",
				mode: fs.ModeDir,
			},
		}, nil
	}

	head, err := s3cl.HeadObject(&s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    aws.String(name),
	})
	if err != nil {
		if !isNotFoundErr(err) {
			return nil, err
		}
	} else {
		return &fileInfo{
			name:    name,
			size:    derefInt64(head.ContentLength),
			mode:    0,
			modTime: derefTime(head.LastModified),
		}, nil
	}

	out, err := s3cl.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    &bucket,
		Delimiter: aws.String("/"),
		Prefix:    aws.String(name + "/"),
		MaxKeys:   aws.Int64(1),
	})
	if err != nil {
		return nil, err
	}
	if len(out.CommonPrefixes) > 0 || len(out.Contents) > 0 {
		return &dir{
			s3cl:   s3cl,
			bucket: bucket,
			fileInfo: fileInfo{
				name: name,
				mode: fs.ModeDir,
			},
		}, nil
	}
	return nil, fs.ErrNotExist
}

func openDir(s3cl s3iface.S3API, bucket, name string) (fs.ReadDirFile, error) {
	fi, err := stat(s3cl, bucket, name)
	if err != nil {
		return nil, err
	}

	if d, ok := fi.(fs.ReadDirFile); ok {
		return d, nil
	}
	return nil, errNotDir
}

var notFoundCodes = map[string]struct{}{
	s3.ErrCodeNoSuchKey: {},
	"NotFound":          {}, // localstack
}

func isNotFoundErr(err error) bool {
	if aerr, ok := err.(awserr.Error); ok {
		_, ok := notFoundCodes[aerr.Code()]
		return ok
	}
	return false
}
