package s3fs

import (
	"errors"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type dir struct {
	fileInfo
	s3cl   s3iface.S3API
	bucket string
	marker *string
	done   bool
	buf    []fs.DirEntry
	dirs   []fs.DirEntry
}

func (d *dir) Stat() (fs.FileInfo, error) {
	return &d.fileInfo, nil
}

func (d *dir) Read([]byte) (int, error) {
	return 0, &fs.PathError{
		Op:   "read",
		Path: d.name,
		Err:  errors.New("is a directory"),
	}
}

func (d *dir) Close() error {
	return nil
}

func (d *dir) ReadDir(n int) (des []fs.DirEntry, err error) {
	if n <= 0 {
		switch err := d.readAll(); {
		case err == nil:
		case errors.Is(err, io.EOF):
			return []fs.DirEntry{}, nil
		default:
			return nil, err
		}

		des, d.buf = d.buf, nil
		return des, nil
	}

loop:
	for len(d.buf) < n {
		switch err := d.readNext(); {
		case err == nil:
			continue
		case errors.Is(err, io.EOF):
			break loop
		default:
			return nil, err
		}
	}

	offset := min(n, len(d.buf))
	des, d.buf = d.buf[:offset], d.buf[offset:]

	if d.done && len(d.buf) == 0 {
		err = io.EOF
	}

	return des, err
}

func (d *dir) readAll() error {
	for !d.done {
		switch err := d.readNext(); {
		case err == nil:
			continue
		case errors.Is(err, io.EOF):
			return nil
		default:
			return err
		}
	}
	return io.EOF
}

func (d *dir) readNext() error {
	if d.done {
		return io.EOF
	}

	name := strings.Trim(d.name, "/")

	switch {
	case name == ".":
		name = ""
	default:
		name += "/"
	}

	out, err := d.s3cl.ListObjects(&s3.ListObjectsInput{
		Bucket:    &d.bucket,
		Delimiter: aws.String("/"),
		Prefix:    &name,
		Marker:    d.marker,
	})
	if err != nil {
		return err
	}

	if d.name != "." && len(out.CommonPrefixes)+len(out.Contents) == 0 {
		return &fs.PathError{
			Op:   "readdir",
			Path: strings.TrimSuffix(name, "/"),
			Err:  fs.ErrNotExist,
		}
	}

	if d.dirs == nil {
		for _, p := range out.CommonPrefixes {
			d.dirs = append(d.dirs, dirEntry{
				fileInfo: fileInfo{
					name:    path.Base(*p.Prefix),
					size:    0,
					mode:    fs.ModeDir,
					modTime: time.Time{},
				},
			})
		}
	}

	d.marker = out.NextMarker
	d.done = out.IsTruncated != nil && !(*out.IsTruncated)

	for _, o := range out.Contents {
		d.buf = append(d.buf, dirEntry{
			fileInfo: fileInfo{
				name:    path.Base(*o.Key),
				size:    *o.Size,
				mode:    0,
				modTime: *o.LastModified,
			},
		})
	}

	var i int
	for ; i < len(d.dirs); i++ {
		i := sort.Search(len(d.buf), func(j int) bool {
			return d.buf[j].Name() >= d.dirs[i].Name()
		})

		if i == len(d.buf) && !d.done {
			break
		}
	}
	d.buf = append(d.buf, d.dirs[:i]...)
	d.dirs = d.dirs[i:]

	sort.Slice(d.buf, func(i, j int) bool {
		return d.buf[i].Name() < d.buf[j].Name()
	})

	if d.done {
		return io.EOF
	}
	return nil
}

type dirEntry struct {
	fileInfo
}

func (de dirEntry) Type() fs.FileMode          { return de.Mode().Type() }
func (de dirEntry) Info() (fs.FileInfo, error) { return de.fileInfo, nil }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
