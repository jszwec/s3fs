// Package s3fs provides a S3 implementation for Go1.16 filesystem interface.
//
package s3fs

import (
	"context"
	"errors"
	"io/fs"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var (
	_ fs.FS        = (*S3FS)(nil)
	_ fs.StatFS    = (*S3FS)(nil)
	_ fs.ReadDirFS = (*S3FS)(nil)
	_ S3Api        = (*s3.Client)(nil)
)

var errNotDir = errors.New("not a dir")

// S3Api is a api interface for all functions needed for this project.
// This is suggested by https://aws.github.io/aws-sdk-go-v2/docs/unit-testing/#mocking-client-operations
type S3Api interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3FS is a S3 filesystem implementation.
//
// S3 has a flat structure instead of a hierarchy. S3FS simulates directories
// by using prefixes and delims ("/"). Because directories are simulated, ModTime
// is always a default Time value (IsZero returns true).
type S3FS struct {
	context context.Context
	s3Api   S3Api
	bucket  string
}

// New returns a new filesystem that works on the specified bucket.
func New(context context.Context, s3Api S3Api, bucket string) *S3FS {
	return &S3FS{
		context: context,
		s3Api:   s3Api,
		bucket:  bucket,
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
		return openDir(f.context, f.s3Api, f.bucket, name)
	}

	out, err := f.s3Api.GetObject(f.context, &s3.GetObjectInput{
		Key:    &name,
		Bucket: &f.bucket,
	})

	if err != nil {
		if isNotFoundErr(err) {
			switch d, err := openDir(f.context, f.s3Api, f.bucket, name); {
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
		return stat(f.context, f.s3Api, f.bucket, name)
	}

	if out.LastModified != nil {
		// if we got all the information from GetObjectOutput
		// then we can cache fileinfo instead of making
		// another call in case Stat is called.
		statFunc = func() (fs.FileInfo, error) {
			return &fileInfo{
				name:    path.Base(name),
				size:    out.ContentLength,
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
	fi, err := stat(f.context, f.s3Api, f.bucket, name)
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
	d, err := openDir(f.context, f.s3Api, f.bucket, name)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  err,
		}
	}
	return d.ReadDir(-1)
}

func stat(context context.Context, s3Api S3Api, bucket, name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}

	if name == "." {
		return &dir{
			context: context,
			s3Api:   s3Api,
			bucket:  bucket,
			fileInfo: fileInfo{
				name: ".",
				mode: fs.ModeDir,
			},
		}, nil
	}
	head, err := s3Api.HeadObject(context, &s3.HeadObjectInput{
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
			size:    head.ContentLength,
			mode:    0,
			modTime: derefTime(head.LastModified),
		}, nil
	}

	out, err := s3Api.ListObjects(context, &s3.ListObjectsInput{
		Bucket:    &bucket,
		Delimiter: aws.String(name + "/"),
		Prefix:    aws.String(name),
		MaxKeys:   1,
	})
	if err != nil {
		return nil, err
	}

	if len(out.CommonPrefixes) > 0 || len(out.Contents) > 0 {
		return &dir{
			context: context,
			s3Api:   s3Api,
			bucket:  bucket,
			fileInfo: fileInfo{
				name: name,
				mode: fs.ModeDir,
			},
		}, nil
	}
	return nil, fs.ErrNotExist
}

func openDir(context context.Context, s3Api S3Api, bucket, name string) (fs.ReadDirFile, error) {
	fi, err := stat(context, s3Api, bucket, name)
	if err != nil {
		return nil, err
	}

	if d, ok := fi.(fs.ReadDirFile); ok {
		return d, nil
	}
	return nil, errNotDir
}

func isNotFoundErr(err error) bool {
	var re *smithyhttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == 404
}
