package s3fs

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io"
	"io/fs"
	"path"
	"time"
)

var (
	_ fs.File     = (*file)(nil)
	_ fs.FileInfo = (*fileInfo)(nil)
	_ io.Seeker   = (*file)(nil)
)

type file struct {
	cl         s3iface.S3API
	bucket     string
	name       string
	realReader io.ReadCloser
	stat       func() (fs.FileInfo, error)

	currentPosition int64
}

func (f *file) Read(p []byte) (int, error) {
	n, err := f.realReader.Read(p)
	f.currentPosition += int64(n)
	return n, err
}

func (f *file) Close() error {
	return f.realReader.Close()
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	newPosition := f.currentPosition
	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition += offset
	case io.SeekEnd:
		stat, err := f.Stat()
		if err != nil {
			return 0, err
		}
		newPosition = stat.Size() + offset
	default:
		return 0, fmt.Errorf("unknown 'whence': %d", whence)
	}
	// If the position has not moved, there is no need to make a new query
	if f.currentPosition == newPosition {
		return newPosition, nil
	}

	rawObject, err := f.cl.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.name),
			Range:  aws.String(fmt.Sprintf("bytes=%d-", newPosition)),
		})

	if err != nil {
		return f.currentPosition, err
	}

	f.currentPosition = newPosition
	f.realReader = rawObject.Body

	return f.currentPosition, nil
}

func (f *file) Stat() (fs.FileInfo, error) { return f.stat() }

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
