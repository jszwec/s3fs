package s3fs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	_ fs.File     = (*file)(nil)
	_ fs.FileInfo = (*fileInfo)(nil)
	_ io.Seeker   = (*file)(nil)
)

type file struct {
	cl     s3iface.S3API
	bucket string
	name   string

	io.ReadCloser
	stat   func() (fs.FileInfo, error)
	offset int64
	eTag   string
}

func openFile(cl s3iface.S3API, bucket string, name string) (fs.File, error) {
	out, err := cl.GetObject(&s3.GetObjectInput{
		Key:    &name,
		Bucket: &bucket,
	})

	if err != nil {
		return nil, err
	}

	statFunc := getStatFunc(cl, bucket, name, *out)

	return &file{
		cl:         cl,
		bucket:     bucket,
		name:       name,
		ReadCloser: out.Body,
		stat:       statFunc,
		offset:     0,
		eTag:       *out.ETag,
	}, nil
}

func getStatFunc(cl s3iface.S3API, bucket string, name string, s3ObjOutput s3.GetObjectOutput) func() (fs.FileInfo, error) {
	statFunc := func() (fs.FileInfo, error) {
		return stat(cl, bucket, name)
	}

	if s3ObjOutput.ContentLength != nil && s3ObjOutput.LastModified != nil {
		// if we got all the information from GetObjectOutput
		// then we can cache fileinfo instead of making
		// another call in case Stat is called.
		statFunc = func() (fs.FileInfo, error) {
			return &fileInfo{
				name:    path.Base(name),
				size:    *s3ObjOutput.ContentLength,
				modTime: *s3ObjOutput.LastModified,
			}, nil
		}
	}

	return statFunc
}

func (f *file) Read(p []byte) (int, error) {
	n, err := f.ReadCloser.Read(p)
	f.offset += int64(n)
	return n, err
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	newOffset := f.offset

	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := stat.Size()

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset += offset
	case io.SeekEnd:
		newOffset = size + offset
	default:
		return 0, errors.New("s3fs.file.Seek: invalid whence")
	}

	// If the position has not moved, there is no need to make a new query
	if f.offset == newOffset {
		return newOffset, nil
	}

	if newOffset < 0 {
		return 0, errors.New("s3fs.file.Seek: seeked to a negative position")
	}

	if f.eTag == "" {
		return 0, errors.New("s3fs.file.Seek: cannot seek. remote file has no etag")
	}

	if err := f.Close(); err != nil {
		return f.offset, err
	}

	if newOffset >= size {
		f.ReadCloser = io.NopCloser(eofReader{})
		f.offset = newOffset
		return f.offset, nil
	}

	rawObject, err := f.cl.GetObject(
		&s3.GetObjectInput{
			Bucket:  aws.String(f.bucket),
			Key:     aws.String(f.name),
			Range:   aws.String(fmt.Sprintf("bytes=%d-", newOffset)),
			IfMatch: aws.String(f.eTag),
		})

	if err != nil {
		var requestFailureError awserr.RequestFailure
		if errors.As(err, &requestFailureError) && requestFailureError.StatusCode() == http.StatusPreconditionFailed {
			return 0, fmt.Errorf("s3fs.file.Seek: file has changed while seeking: %w", fs.ErrNotExist)
		}
		return 0, err
	}

	f.offset = newOffset
	f.ReadCloser = rawObject.Body

	return f.offset, nil
}

func (f file) Stat() (fs.FileInfo, error) { return f.stat() }

type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
}

func (fi fileInfo) Name() string       { return path.Base(fi.name) }
func (fi fileInfo) Size() int64        { return fi.size }
func (fi fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi fileInfo) ModTime() time.Time { return fi.modTime }
func (fi fileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi fileInfo) Sys() interface{}   { return nil }

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }
