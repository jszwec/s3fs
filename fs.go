package s3fs

import (
	"io/fs"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	_ fs.FS     = (*FS)(nil)
	_ fs.StatFS = (*FS)(nil)
)

// FS is a S3 filesystem implementation.
type FS struct {
	cl     s3iface.S3API
	bucket string
}

// New returns a new filesystem that works on the specified bucket.
func New(cl s3iface.S3API, bucket string) *FS {
	return &FS{
		cl:     cl,
		bucket: bucket,
	}
}

// Open opens an S3 file and wraps it into filesystem File.
//
// It returns PathError immediately if fs.ValidPath returns false.
//
// Open currently doesn't support "." path.
func (f *FS) Open(name string) (fs.File, error) {
	if name == "." || !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	out, err := f.cl.GetObject(&s3.GetObjectInput{
		Key:    &name,
		Bucket: &f.bucket,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
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

	statFunc := func() (*fileInfo, error) {
		return stat(f.cl, f.bucket, name)
	}

	if out.ContentLength != nil && out.LastModified != nil {
		// if we got all the information from GetObjectOutput
		// then we can cache fileinfo instead of making
		// another call in case Stat is called.
		statFunc = func() (*fileInfo, error) {
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

// Stat implements fs.StatFS. It wraps S3 object information
// into fs.FileInfo.
//
// It returns PathError immediately if fs.ValidPath returns false.
//
// Stat currently doesn't support "." path.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	if name == "." || !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	return stat(f.cl, f.bucket, name)
}

func stat(s3cl s3iface.S3API, bucket, name string) (*fileInfo, error) {
	h, err := s3cl.HeadObject(&s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &name,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return nil, &fs.PathError{
				Op:   "stat",
				Path: name,
				Err:  fs.ErrNotExist,
			}
		}

		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}

	var size int64
	if h.ContentLength != nil {
		size = *h.ContentLength
	}

	var modTime time.Time
	if h.LastModified != nil {
		modTime = *h.LastModified
	}

	return &fileInfo{
		name:    path.Base(name),
		size:    size,
		modTime: modTime,
	}, nil
}
