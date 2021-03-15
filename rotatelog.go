package glog

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultBackupTimeFormat = "2006-01-02 15:04:05"
	defaultMaxSize          = 100
	defaultDirMode          = 0755
	defaultFileMode         = 0600
	compressSuffix          = ".gz"
)

type (
	// RotateLog is an io.WriteCloser than writes to the specified filename.
	// It provides a rolling log.
	RotateLog struct {
		// Filename the file to write logs to.
		Filename string
		// MaxBackUps the maximum number of old log file to retain based. The default is
		// to retain all old log files.
		MaxBackUps int
		// MaxSize the maximum size in megabytes of the log file before it gets rotated.
		// It default to 100 megabytes.
		MaxSize int
		// MaxAge the maximum number of days to retain old log file based on the timestamp
		// encoded in their filename.
		MaxAge int
		// RotateByTimeOrSize the log file rotate way, by time or size, one or the other.
		// the "time" value indicate the log file rotate by specified time interval.
		// the "size" value indicate the log file rotate by MaxSize size.
		// default value is "time"
		RotateByTimeOrSize string
		// Compress determines if the rotated log files should be compressed
		// using gzip. The default is not to perform compression.
		Compress bool

		LocalTime bool

		// mu concurrent writes is locked.
		mu sync.Mutex
		// size the number of megabytes to exists log file.
		size      int64
		file      *os.File
		startMill sync.Once
		millCh    chan bool
	}

	logInfo struct {
		timestamp time.Time
		os.FileInfo
	}
)

var (
	// megabyte the conversion factor between MaxSize and bytes.
	megabyte = 1024 * 1024
)

// Write implements io.Writer. If a write would cause the log file to be larger
// than MaxSize, the file is closed, renamed to include a timestamp of the current
// time, and a new log file is created using the original log file name.
func (r *RotateLog) Write(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	writeLen := int64(len(p))
	if writeLen > r.max() {
		return 0, fmt.Errorf("wirte length %d exceeds maximum file size %d", writeLen, r.max())
	}

	if r.file == nil {
		if err := r.openExistingOrNew(len(p)); err != nil {
			return 0, err
		}
	}

	if r.size+writeLen > r.max() {
		if err := r.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = r.file.Write(p)
	r.size += int64(n)

	return n, err
}

// openExistingOrNew open the logfile if it exist and if the current write
// would not put it over MaxSize. If there is no such file or the write would
// put it over MaxSize, a new file is created.
func (r *RotateLog) openExistingOrNew(writeLen int) error {
	r.mill()

	filename := r.filename()
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return r.openNew()
	}
	if err != nil {
		return fmt.Errorf("error getting log file info: %s", err)
	}

	if info.Size()+int64(writeLen) >= r.max() {
		return r.rotate()
	}

	file, err := os.OpenFile(filename, os.O_APPEND+os.O_WRONLY, defaultFileMode)
	if err != nil {
		return r.openNew()
	}
	r.file = file
	r.size = info.Size()
	return nil
}

// rotate close the current file, moves it aside with a timestamp in the name
// if it exist, opens a new file with the original filename and then runs
// post-rotation processing and removal.
func (r *RotateLog) rotate() error {
	if err := r.close(); err != nil {
		return err
	}

	if err := r.openNew(); err != nil {
		return err
	}
	r.mill()
	return nil
}

// close close the file if it is open.
func (r *RotateLog) close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

// openNew opens a new log file for writing, moving any old log file out of the way.
// This method assumes the file has already been closed.
func (r *RotateLog) openNew() error {
	err := os.MkdirAll(r.dir(), defaultDirMode)
	if err != nil {
		return err
	}

	name := r.filename()
	mode := os.FileMode(defaultFileMode)
	info, err := os.Stat(name)
	if err == nil {
		mode = info.Mode()
		newName := backupName(name, r.LocalTime)
		if err := os.Rename(name, newName); err != nil {
			return fmt.Errorf("can't rename log file: %s", err)
		}
	}

	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("can't open new logfile: %s", err)
	}
	r.file = f
	r.size = 0
	return nil
}

// mill performs post-rotation compression and removal of stale log files.
func (r *RotateLog) mill() {
	r.startMill.Do(func() {
		r.millCh = make(chan bool, 1)
		go r.millRun()
	})
	select {
	case r.millCh <- true:
	default:
	}
}

// filename returns generates the name of logfile from the current time.
func (r *RotateLog) filename() string {
	if r.Filename != "" {
		return r.Filename
	}
	name := filepath.Base(os.Args[0]) + "_rotate.log"
	return filepath.Join(os.TempDir(), name)
}

// dir returns the directory for current filename.
func (r *RotateLog) dir() string {
	return filepath.Dir(r.Filename)
}

// backupName returns a new filename from the given name, inserting a timestamp
// between the filename and the extension, using the local time if requested
// otherwise UTC.
func backupName(name string, local bool) string {
	dir := filepath.Dir(name)
	filename := filepath.Base(name)
	ext := filepath.Ext(name)
	prefix := filename[:len(filename)-len(ext)]
	t := time.Now()
	if !local {
		t = t.UTC()
	}

	timestamp := t.Format(defaultBackupTimeFormat)
	return filepath.Join(dir, fmt.Sprintf("%s-%s%s", prefix, timestamp, ext))
}

// millRun runs in a goroutine to manage post-rotation compression and removal
// of old log files.
func (r *RotateLog) millRun() {
	for range r.millCh {
		_ = r.millRunOnce()
	}
}

// millRunOnce performs compression and removal of stale log files.
// Log files are compressed if enabled via configuration and old log files
// are removed, keeping at most MaxBackups files, as long as none of them
// are older than MaxAge.
func (r *RotateLog) millRunOnce() error {
	if r.MaxSize == 0 && r.MaxAge == 0 && !r.Compress {
		return nil
	}

	files, err := r.oldLogFiles()
	if err != nil {
		return nil
	}

	var compress, remove []logInfo

	if r.MaxBackUps > 0 && r.MaxBackUps < len(files) {
		preserved := make(map[string]bool)
		var remaining []logInfo
		for _, f := range files {
			// Only count the uncompressed log file or
			// the compressed log file, no both.
			fn := f.Name()
			if strings.HasSuffix(fn, compressSuffix) {
				fn = fn[:len(fn)-len(compressSuffix)]
			}
			preserved[fn] = true

			if len(preserved) > r.MaxBackUps {
				remove = append(remove, f)
			} else {
				remaining = append(remaining, f)
			}
		}
	}
	if r.MaxAge > 0 {
		diff := time.Duration(int64(24*time.Hour) * int64(r.MaxAge))
		cutOff := time.Now().Add(-1 * diff)

		var remaining []logInfo
		for _, f := range files {
			if f.timestamp.Before(cutOff) {
				remove = append(remove, f)
			} else {
				remaining = append(remaining, f)
			}
		}
		files = remaining
	}

	if r.Compress {
		for _, f := range files {
			if !strings.HasSuffix(f.Name(), compressSuffix) {
				compress = append(compress, f)
			}
		}
	}

	for _, f := range remove {
		errRemove := os.Remove(filepath.Join(r.dir(), f.Name()))
		if err == nil && errRemove != nil {
			err = errRemove
		}
	}

	for _, f := range compress {
		fn := filepath.Join(r.dir(), f.Name())
		errCompress := compressLogFile(fn, fn+compressSuffix)
		if err == nil && errCompress != nil {
			err = errCompress
		}
	}

	return err
}

// oldLogFiles returns the list of backup log files stored in the same directory
// as the current log file, sorted by ModTime.
func (r *RotateLog) oldLogFiles() ([]logInfo, error) {
	files, err := ioutil.ReadDir(r.dir())
	if err != nil {
		return nil, fmt.Errorf("can't read log file directory: %s", err)
	}

	var logFiles []logInfo
	prefix, ext := r.prefixAndExt()

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if t, err := r.timeFromName(f.Name(), prefix, ext); err == nil {
			logFiles = append(logFiles, logInfo{t, f})
		}
		if t, err := r.timeFromName(f.Name(), prefix, ext+compressSuffix); err == nil {
			logFiles = append(logFiles, logInfo{t, f})
		}
	}

	sort.Sort(byFormatTime(logFiles))

	return logFiles, nil
}

// timeFromName extracts the formatted time from filename by stripping off
// the filename's prefix and extension.
func (r *RotateLog) timeFromName(filename, prefix, ext string) (time.Time, error) {
	if !strings.HasPrefix(filename, prefix) {
		return time.Time{}, errors.New("mismatched prefix")
	}
	if !strings.HasSuffix(filename, ext) {
		return time.Time{}, errors.New("mismatched extension")
	}
	ts := filename[len(prefix) : len(filename)-len(ext)]
	return time.Parse(defaultBackupTimeFormat, ts)
}

func (r *RotateLog) prefixAndExt() (prefix, ext string) {
	filename := filepath.Base(r.filename())
	ext = filepath.Ext(filename)
	prefix = filename[:len(filename)-len(ext)] + "-"
	return
}

// max returns the maximum size in bytes of log file before rolling.
func (r *RotateLog) max() int64 {
	if r.MaxSize == 0 {
		return int64(defaultMaxSize * megabyte)
	}
	return int64(r.MaxSize * megabyte)
}

// compressLogFile compress the given log file, removing the
// uncompressed log file if successful.
func compressLogFile(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer f.Close()

	fi, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat log file: %v", err)
	}

	gzf, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fi.Mode())
	if err != nil {
		return fmt.Errorf("failed to open compressed log file: %v", err)
	}
	defer gzf.Close()

	gz := gzip.NewWriter(gzf)
	defer func() {
		if err != nil {
			os.Remove(dst)
			err = fmt.Errorf("failed to compress log file: %v", err)
		}
	}()

	if _, err := io.Copy(gz, f); err != nil {
		return err
	}

	if err := gz.Close(); err != nil {
		return err
	}

	if err := gzf.Close(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return nil
}

type byFormatTime []logInfo

func (b byFormatTime) Less(i, j int) bool {
	return b[i].timestamp.After(b[j].timestamp)
}

func (b byFormatTime) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byFormatTime) Len() int {
	return len(b)
}
