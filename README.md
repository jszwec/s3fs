# s3fs [![Go Reference](https://pkg.go.dev/badge/github.com/jszwec/s3fs.svg)](https://pkg.go.dev/github.com/jszwec/s3fs) ![Go](https://github.com/jszwec/s3fs/workflows/Go/badge.svg?branch=main)

Package s3fs provides a S3 implementation for Go1.16 [filesystem](https://tip.golang.org/pkg/io/fs/#FS) interface.

Since S3 is a flat structure, s3fs simulates directories by using
prefixes and "/" delim. ModTime on directories is always zero value.

The implementation wraps [aws sdk go](https://github.com/aws/aws-sdk-go) s3 client.

```go
const bucket = "my-bucket"

s, err := session.NewSession()
if err != nil {
    log.Fatal(err)
}

s3fs := s3fs.New(s3.New(s), bucket)

// print out all files in s3 bucket.
_ = fs.WalkDir(s3fs, ".", func(path string, d fs.DirEntry, err error) error {
    if err != nil {
        return err
    }

    if d.IsDir() {
        fmt.Println("dir:", path)
        return nil
    }
    fmt.Println("file:", path)
    return nil
})
```

# Installation

```
go get github.com/jszwec/s3fs
```

# Requirements

* Go1.16+
