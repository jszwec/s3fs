package s3fs

import "io/fs"

type dirEntry struct {
	fileInfo
}

func (de dirEntry) Type() fs.FileMode          { return de.Mode() }
func (de dirEntry) Info() (fs.FileInfo, error) { return de.fileInfo, nil }
