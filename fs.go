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
	_ fs.FS        = (*FS)(nil)
	_ fs.StatFS    = (*FS)(nil)
	_ fs.ReadDirFS = (*FS)(nil)
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
			case !isNotFoundErr(err):
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

// Stat implements fs.StatFS. It wraps S3 object information
// into fs.FileInfo.
//
// It returns PathError immediately if fs.ValidPath returns false.
//
// Stat currently doesn't support "." path.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
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

// ReadDir implements fs.ReadDirFS. It wraps S3 objects and Common Prefixes into
// fs.DirEntry.
//
// It returns PathError immediately if fs.ValidPath returns false.
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	fi, err := stat(f.cl, f.bucket, name)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  err,
		}
	}

	d, ok := fi.(fs.ReadDirFile)
	if !fi.IsDir() || !ok {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  errors.New("not a dir"),
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

	out, err := s3cl.ListObjects(&s3.ListObjectsInput{
		Bucket:    &bucket,
		Delimiter: aws.String("/"),
		Prefix:    &name,
		MaxKeys:   aws.Int64(1),
	})
	if err != nil {
		return nil, err
	}

	if len(out.CommonPrefixes) != 0 && *out.CommonPrefixes[0].Prefix == name+"/" {
		return &dir{
			s3cl:   s3cl,
			bucket: bucket,
			fileInfo: fileInfo{
				name: name,
				mode: fs.ModeDir,
			},
		}, nil
	}

	if len(out.Contents) != 0 &&
		out.Contents[0].Key != nil &&
		*out.Contents[0].Key == name {
		return &fileInfo{
			name:    name,
			size:    *out.Contents[0].Size,
			mode:    0,
			modTime: *out.Contents[0].LastModified,
		}, nil
	}

	return nil, fs.ErrNotExist
}

var errNotFound = errors.New("not found")

func openDir(s3cl s3iface.S3API, bucket, prefix string) (*dir, error) {
	out, err := s3cl.ListObjects(&s3.ListObjectsInput{
		Bucket:    &bucket,
		Delimiter: aws.String("/"),
		Prefix:    &prefix,
		MaxKeys:   aws.Int64(0),
	})
	if err != nil {
		return nil, err
	}

	if len(out.CommonPrefixes) != 0 && *out.CommonPrefixes[0].Prefix != prefix+"/" {
		return nil, errNotFound
	}

	return &dir{
		s3cl:   s3cl,
		bucket: bucket,
		fileInfo: fileInfo{
			name: prefix,
			mode: fs.ModeDir,
		},
	}, nil
}

var notFoundCodes = map[string]struct{}{
	s3.ErrCodeNoSuchKey: {},
	"NotFound":          {},
}

func isNotFoundErr(err error) bool {
	if aerr, ok := err.(awserr.Error); ok {
		_, ok := notFoundCodes[aerr.Code()]
		return ok
	}
	return false
}
