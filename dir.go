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

var _ fs.ReadDirFile = (*dir)(nil)

type dir struct {
	fileInfo
	s3cl   s3iface.S3API
	bucket string
	marker *string
	done   bool
	buf    []fs.DirEntry
	dirs   map[dirEntry]bool
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
	des, d.buf = d.buf[:offset:offset], d.buf[offset:]

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

	name := strings.TrimRight(d.name, "/")
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

	d.marker = out.NextMarker
	d.done = out.IsTruncated != nil && !(*out.IsTruncated)

	if d.dirs == nil {
		d.dirs = make(map[dirEntry]bool)
	}

	for _, p := range out.CommonPrefixes {
		if p == nil || p.Prefix == nil {
			continue
		}

		de := dirEntry{
			fileInfo: fileInfo{
				name: path.Base(*p.Prefix),
				mode: fs.ModeDir,
			},
		}

		if _, ok := d.dirs[de]; !ok {
			d.dirs[de] = false
		}
	}

	for _, o := range out.Contents {
		if o == nil || o.Key == nil {
			continue
		}

		d.buf = append(d.buf, dirEntry{
			fileInfo: fileInfo{
				name:    path.Base(*o.Key),
				size:    derefInt64(o.Size),
				modTime: derefTime(o.LastModified),
			},
		})
	}

	d.mergeDirFiles()

	if d.done {
		return io.EOF
	}
	return nil
}

func (d *dir) mergeDirFiles() {
	if d.buf == nil {
		// according to fs docs ReadDir should never return nil slice,
		// so we set it here.
		d.buf = []fs.DirEntry{}
	}

	// we need a current len for sort.Search that doesn't change; otherwise
	// we could not append to the same slice.
	l := len(d.buf)
	for de, used := range d.dirs {
		if used {
			continue
		}

		i := sort.Search(l, func(i int) bool {
			return d.buf[i].Name() >= de.Name()
		})

		if i == l && !d.done {
			continue
		}
		d.buf = append(d.buf, de)
		d.dirs[de] = true
	}

	sort.Slice(d.buf, func(i, j int) bool {
		return d.buf[i].Name() < d.buf[j].Name()
	})
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

func derefInt64(n *int64) int64 {
	if n != nil {
		return *n
	}
	return 0
}

func derefTime(t *time.Time) time.Time {
	if t != nil {
		return *t
	}
	return time.Time{}
}
