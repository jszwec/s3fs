package s3fs_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jszwec/s3fs"
)

func TestDirRead(t *testing.T) {
	type fileinfo struct {
		name  string
		isDir bool
	}

	tests := []struct {
		desc     string
		n        int
		outs     []s3.ListObjectsOutput
		expected [][]fileinfo
	}{
		{
			desc: "all in one request - dir first",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c", "e"}, []string{"b", "d", "f"}),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"b", false}},
				{{"c", true}},
				{{"d", false}},
				{{"e", true}},
				{{"f", false}},
			},
		},
		{
			desc: "all in one request - n = 0",
			n:    0,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c", "e"}, []string{"b", "d", "f"}),
			},
			expected: [][]fileinfo{
				{
					{"a", true},
					{"b", false},
					{"c", true},
					{"d", false},
					{"e", true},
					{"f", false},
				},
			},
		},
		{
			desc: "all in one request - n = 2",
			n:    2,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a"}, nil),
				newListOutput([]string{"c"}, []string{"b", "d"}),
				newListOutput([]string{"e"}, nil),
				newListOutput(nil, []string{"f"}),
			},
			expected: [][]fileinfo{
				{
					{"a", true},
					{"b", false},
				},
				{
					{"c", true},
					{"d", false},
				},
				{
					{"e", true},
					{"f", false},
				},
			},
		},
		{
			desc: "one per request - dir first",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a"}, nil),
				newListOutput(nil, []string{"b"}),
				newListOutput([]string{"c"}, []string{"d"}),
				newListOutput([]string{"e"}, nil),
				newListOutput(nil, []string{"f"}),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"b", false}},
				{{"c", true}},
				{{"d", false}},
				{{"e", true}},
				{{"f", false}},
			},
		},
		{
			desc: "all in one request - file first",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"b", "d", "f"}, []string{"a", "c", "e"}),
			},
			expected: [][]fileinfo{
				{{"a", false}},
				{{"b", true}},
				{{"c", false}},
				{{"d", true}},
				{{"e", false}},
				{{"f", true}},
			},
		},
		{
			desc: "with dir duplicates",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c"}, []string{"b"}),
				newListOutput([]string{"c", "e", "c"}, []string{"d"}),
				newListOutput([]string{"e", "a"}, []string{"f"}),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"b", false}},
				{{"c", true}},
				{{"d", false}},
				{{"e", true}},
				{{"f", false}},
			},
		},

		{
			desc: "all in one request - dirs only",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a", "c", "e"}, nil),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"c", true}},
				{{"e", true}},
			},
		},
		{
			desc: "single dir per request - dirs only",
			n:    1,
			outs: []s3.ListObjectsOutput{
				newListOutput([]string{"a"}, nil),
				newListOutput([]string{"c"}, nil),
				newListOutput([]string{"e"}, nil),
			},
			expected: [][]fileinfo{
				{{"a", true}},
				{{"c", true}},
				{{"e", true}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			f, err := s3fs.New(context.TODO(), &mockClient{
				outs: test.outs,
			}, "test").Open(".")
			if err != nil {
				t.Fatal("expected err to be nil; got ", err)
			}

			fi, err := f.Stat()
			if err != nil {
				t.Fatal("expected err to be nil; got ", err)
			}

			if !fi.IsDir() {
				t.Fatal("expected the file to be a directory")
			}

			var fis [][]fileinfo
			for {
				files, err := f.(fs.ReadDirFile).ReadDir(test.n)
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatal("did not expect err:", err)
				}

				if len(files) > 0 {
					var out []fileinfo
					for _, f := range files {
						out = append(out, fileinfo{f.Name(), f.IsDir()})
					}
					fis = append(fis, out)
				}

				if test.n <= 0 || errors.Is(err, io.EOF) {
					break
				}
			}

			if !reflect.DeepEqual(fis, test.expected) {
				t.Errorf("want %v; got %v", test.expected, fis)
			}
		})
	}
}

type mockClient struct {
	s3fs.S3Api

	outs []s3.ListObjectsOutput
	i    int
}

func (c *mockClient) ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	defer func() { c.i++ }()
	if c.i < len(c.outs) {
		return &c.outs[c.i], nil
	}

	return &s3.ListObjectsOutput{
		IsTruncated: false,
	}, nil
}

func newListOutput(dirs, files []string) (out s3.ListObjectsOutput) {
	for _, d := range dirs {
		out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{
			Prefix: aws.String(d),
		})
	}

	for _, f := range files {
		out.Contents = append(out.Contents, types.Object{
			Key:          aws.String(f),
			Size:         0,
			LastModified: aws.Time(time.Time{}),
		})
	}
	out.IsTruncated = true
	return out
}
