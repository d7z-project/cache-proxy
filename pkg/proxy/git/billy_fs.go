package git

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"sync/atomic"

	"github.com/go-git/go-billy/v5"
	"github.com/spf13/afero"
)

var tempSeq atomic.Uint64

type billyAdapter struct {
	fs  afero.Fs
	dir string
}

func newBillyAdapter(afs afero.Fs, dir string) *billyAdapter {
	return &billyAdapter{fs: afs, dir: dir}
}

func (b *billyAdapter) Create(name string) (billy.File, error) {
	if err := b.MkdirAll(b.Dir(name), 0o755); err != nil {
		return nil, err
	}
	f, err := b.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &billyFile{File: f}, nil
}

func (b *billyAdapter) Open(name string) (billy.File, error) {
	f, err := b.fs.Open(name)
	if err != nil {
		return nil, err
	}
	return &billyFile{File: f}, nil
}

func (b *billyAdapter) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	if flag&os.O_CREATE != 0 {
		if err := b.MkdirAll(b.Dir(name), 0o755); err != nil {
			return nil, err
		}
	}
	f, err := b.fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &billyFile{File: f}, nil
}

func (b *billyAdapter) Stat(name string) (os.FileInfo, error) {
	return b.fs.Stat(name)
}

func (b *billyAdapter) Rename(oldname, newname string) error {
	return b.fs.Rename(oldname, newname)
}

func (b *billyAdapter) Remove(name string) error {
	return b.fs.Remove(name)
}

func (b *billyAdapter) Join(elem ...string) string {
	return path.Join(elem...)
}

func (b *billyAdapter) Dir(name string) string {
	return path.Dir(name)
}

func (b *billyAdapter) Base(name string) string {
	return path.Base(name)
}

func (b *billyAdapter) TempFile(dir, prefix string) (billy.File, error) {
	if err := b.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	name := path.Join(dir, prefix+strconv.FormatUint(tempSeq.Add(1), 36))
	f, err := b.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &billyFile{File: f}, nil
}

func (b *billyAdapter) ReadDir(name string) ([]os.FileInfo, error) {
	f, err := b.fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdir(-1)
}

func (b *billyAdapter) MkdirAll(name string, perm os.FileMode) error {
	return b.fs.MkdirAll(name, perm)
}

func (b *billyAdapter) Lstat(name string) (os.FileInfo, error) {
	return b.fs.Stat(name)
}

func (b *billyAdapter) Symlink(target, link string) error {
	return fmt.Errorf("symlink not supported")
}

func (b *billyAdapter) Readlink(link string) (string, error) {
	return "", fmt.Errorf("symlink not supported")
}

func (b *billyAdapter) Chroot(subPath string) (billy.Filesystem, error) {
	return &billyAdapter{
		fs:  afero.NewBasePathFs(b.fs, subPath),
		dir: subPath,
	}, nil
}

func (b *billyAdapter) Root() string {
	return b.dir
}

type billyFile struct {
	afero.File
}

func (f *billyFile) Lock() error   { return nil }
func (f *billyFile) Unlock() error { return nil }

var _ billy.Filesystem = (*billyAdapter)(nil)
var _ billy.File = (*billyFile)(nil)

func _checkInterfaces() {
	var _ io.Seeker = (&billyFile{})
	var _ io.ReaderAt = (&billyFile{})
	var _ io.WriterAt = (&billyFile{})
	var _ fs.File = (&billyFile{})
}
