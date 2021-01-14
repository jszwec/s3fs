package s3fs

import (
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

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

// ReadDir implements fs.ReadDirFS. It wraps S3 objects and Common Prefixes into
// fs.DirEntry.
//
// It returns PathError immediately if fs.ValidPath returns false.
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	name = strings.Trim(name, "/")

	switch {
	case name == ".":
		name = ""
	default:
		name += "/"
	}

	prefixes, objects, err := listObjects(f.cl, f.bucket, name, -1)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: strings.TrimSuffix(name, "/"),
			Err:  err,
		}
	}

	if len(prefixes)+len(objects) == 0 {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: strings.TrimSuffix(name, "/"),
			Err:  fs.ErrNotExist,
		}
	}

	des := make([]fs.DirEntry, 0, len(prefixes)+len(objects))
	for _, p := range prefixes {
		des = append(des, dirEntry{
			fileInfo: fileInfo{
				name:    path.Base(*p.Prefix),
				size:    0,
				mode:    fs.ModeDir,
				modTime: time.Time{},
			},
		})
	}

	for _, o := range objects {
		des = append(des, dirEntry{
			fileInfo: fileInfo{
				name:    path.Base(*o.Key),
				size:    *o.Size,
				mode:    0,
				modTime: *o.LastModified,
			},
		})
	}

	sort.Slice(des, func(i, j int) bool {
		return des[i].Name() < des[j].Name()
	})
	return des, nil
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

func listObjects(s3cl s3iface.S3API, bucket, prefix string, max int64) (prefixes []*s3.CommonPrefix, objects []*s3.Object, err error) {
	var maxKeys *int64
	if max > 1 {
		maxKeys = aws.Int64(max)
	}

	for marker := (*string)(nil); ; {
		out, err := s3cl.ListObjects(&s3.ListObjectsInput{
			Bucket:    &bucket,
			Delimiter: aws.String("/"),
			Prefix:    &prefix,
			Marker:    marker,
			MaxKeys:   maxKeys,
		})
		if err != nil {
			return nil, nil, err
		}

		prefixes = append(prefixes, out.CommonPrefixes...)
		objects = append(objects, out.Contents...)

		if out.IsTruncated != nil && !(*out.IsTruncated) {
			return prefixes, objects, nil
		}
		marker = out.Marker
	}
}
