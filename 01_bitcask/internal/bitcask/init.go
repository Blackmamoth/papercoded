package bitcask

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Bitcask struct {
	dirPath          string
	maxFileSize      int64 // in MB
	counter          uint64
	mutex            sync.Mutex
	once             sync.Once
	activeFileHandle *os.File
	keydir           Keydir
	ActiveFileID     string
	ActiveFileSize   int64
}

func NewBitcask(dirPath string, maxFileSize int64) (*Bitcask, error) {
	db := &Bitcask{
		dirPath:     dirPath,
		maxFileSize: maxFileSize * 1024 * 1024,
		keydir:      make(Keydir),
	}

	if err := db.Open(); err != nil {
		return nil, err
	}

	slog.Info("Bitcask instance opened")

	return db, nil
}

func (b *Bitcask) Open() error {

	var openErr error = nil

	b.once.Do(func() {

		if err := b.ensureDirectoryExists(); err != nil {
			openErr = err
			return
		}

		files, err := os.ReadDir(b.dirPath)
		if err != nil {
			slog.Error("error reading bitcask directory", "error", err)
			openErr = err
			return
		}

		if len(files) == 0 {
			b.counter = 0
			if err := b.createNewActiveFile(); err != nil {
				openErr = err
				return
			}
		} else {

			n := 0

			for _, file := range files {
				if !file.IsDir() && strings.HasSuffix(file.Name(), ".data") {
					files[n] = file
					n++
				}
			}

			files = files[:n]

			if len(files) == 0 {
				slog.Info("No data files found in the Bitcask directory, creating new data file")

				b.counter = 0
				openErr = b.createNewActiveFile()
				return
			}

			if err := b.initializeKeydir(files); err != nil {
				slog.Error("failed to initialize keydir", "error", err)
				openErr = err
				return
			}

			sort.Slice(files, func(i, j int) bool {
				return files[i].Name() < files[j].Name()
			})

			activeFile := files[len(files)-1]

			b.counter, err = b.getLatestCount(activeFile)
			if err != nil {
				slog.Error("failed to get latest file id", "error", err)
				openErr = err
				return
			}

			activeFileInfo, err := activeFile.Info()
			if err != nil {
				slog.Error("failed to retrieve stats for current active data file in bitcask directory", "error", err)
				openErr = err
				return
			}

			activeFileSize := activeFileInfo.Size()

			b.ActiveFileSize = activeFileSize

			if activeFileSize >= b.maxFileSize {
				if err := b.createNewActiveFile(); err != nil {
					openErr = err
					return
				}
			} else {

				file, err := os.OpenFile(filepath.Join(b.dirPath, activeFile.Name()), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					slog.Error("failed to open active data file in bitcask directory", "error", err)
					openErr = err
					return
				}

				b.activeFileHandle = file
				b.ActiveFileID = activeFile.Name()
			}
		}
	})

	return openErr
}

func (b *Bitcask) Put(key, value string) error {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	entry, err := b.appendRecord(key, value)

	if err != nil {
		slog.Error("failed to append record to current active data file", "key", key, "value", value, "active_file_id", b.ActiveFileID)
		return fmt.Errorf("failed to append record for key %s: %v", key, err)
	}

	b.keydir.Set(key, entry)

	slog.Info("set key in keydir", "key", key)

	return nil
}

func (b *Bitcask) Get(key string) (string, bool, error) {

	keydirEntry, ok := b.keydir.Get(key)
	if !ok {
		slog.Warn("attempted to GET non-existent key", "key", key)
		return "", false, nil
	}

	fileID := keydirEntry.fileID

	f, err := os.Open(filepath.Join(b.dirPath, fileID))
	if err != nil {
		slog.Error("failed to open data file for reading value", "file_id", fileID, "error", err)
		return "", false, err
	}
	defer f.Close()

	valueBuf := make([]byte, keydirEntry.valueSize)

	_, err = f.Seek(keydirEntry.valueOffset, io.SeekStart)
	if err != nil {
		slog.Error("failed to seek to value offset in data file", "file_id", fileID, "offset", keydirEntry.valueOffset, "error", err)
		return "", false, fmt.Errorf("seek failed for file %s at offset %d: %w", fileID, keydirEntry.valueOffset, err)
	}

	_, err = io.ReadFull(f, valueBuf)
	if err != nil {
		slog.Error("failed to read value buffer from data file", "file_id", fileID, "error", err)
		return "", false, err
	}

	return string(valueBuf), true, nil
}

func (b *Bitcask) Delete(key string) error {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	_, ok := b.keydir.Get(key)
	if !ok {
		slog.Error("attempted to delete non-existent key", "key", key)
		return fmt.Errorf("attempated to delete non-existent key [%s]", key)
	}

	_, err := b.appendRecord(key, "")

	if err != nil {
		slog.Error("failed to append record to current active data file", "key", key, "value", "<EMPTY>", "active_file_id", b.ActiveFileID)
		return fmt.Errorf("failed to append record for key %s: %v", key, err)
	}

	delete(b.keydir, key)

	slog.Info("DELETE key from keydir", "key", key)

	return nil
}

func (b *Bitcask) Close() error {
	if b.activeFileHandle == nil {
		return nil
	}

	if err := b.activeFileHandle.Close(); err != nil {
		slog.Error("failed to close current active data file", "error", err)
		return err
	}

	b.activeFileHandle = nil
	return nil
}

func (b *Bitcask) ensureDirectoryExists() error {
	info, err := os.Stat(b.dirPath)

	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(b.dirPath, 0755)
			if err != nil {
				slog.Error("failed to create bitcask directory", "error", err)
				return err
			}
		} else {
			slog.Error("failed to retrieve stats for bitcask directory", "error", err)
			return err
		}
	} else if !info.IsDir() {
		slog.Error("provided path is not a directory")
		return fmt.Errorf("provided path is not a directory")
	}

	return nil
}

// func (b *Bitcask) newFileID() uint64 {
// 	return atomic.AddUint64(&b.counter, 1)
// }

func (b *Bitcask) createNewActiveFile() error {

	if err := b.ensureDirectoryExists(); err != nil {
		slog.Error("failed to ensure bitcask directory exists", "error", err)
		return err
	}

	b.counter++
	fileID := fmt.Sprintf("%020d", b.counter)
	fileName := fmt.Sprintf("%s.data", fileID)

	file, err := os.OpenFile(filepath.Join(b.dirPath, fileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("failed to create new file in bitcask directory", "error", err)
		return err
	}

	b.activeFileHandle = file
	b.ActiveFileID = fileName
	b.ActiveFileSize = 0

	return nil
}

func (b *Bitcask) getLatestCount(dirEntry os.DirEntry) (uint64, error) {
	name := dirEntry.Name()

	name = strings.TrimSuffix(name, ".data")

	id, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		slog.Error("failed to parse uint file id", "error", err)
		return 0, err
	}

	return id, nil
}

func (b *Bitcask) initializeKeydir(files []os.DirEntry) error {

	for _, file := range files {
		var offset int64 = 0

		f, err := os.Open(filepath.Join(b.dirPath, file.Name()))
		if err != nil {
			slog.Error("failed to read a data file while initializing keydir", "file_name", file.Name(), "error", err)
			return err
		}

		// reader := bufio.NewReader(f)

		header := make([]byte, 20)
		for {
			_, err := io.ReadFull(f, header)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			if err != nil {
				slog.Error("failed to read current buffer in data file", "file_name", file.Name(), "error", err)
				return err
			}

			klen := int64(binary.LittleEndian.Uint32(header[12:16]))
			vlen := int64(binary.LittleEndian.Uint32(header[16:20]))

			keyBuf := make([]byte, klen)
			if _, err := io.ReadFull(f, keyBuf); err != nil {
				slog.Error("failed to read key buffer", "file_name", file.Name(), "error", err)
				return err
			}

			key := string(keyBuf)

			valueOffset := offset + 20 + klen

			if vlen == 0 {
				delete(b.keydir, key)
			} else {
				b.keydir.Set(key, KeydirEntry{
					fileID:      file.Name(),
					valueOffset: valueOffset,
					valueSize:   vlen,
					timestamp:   int64(binary.LittleEndian.Uint64(header[4:12])),
				})
			}

			slog.Info("SET key [%s] in keydir", "key", key)

			if _, err := f.Seek(vlen, io.SeekCurrent); err != nil {
				slog.Error("failed to skip value offset", "file_name", file.Name(), "error", err)
				return err
			}

			offset += 20 + klen + vlen

		}
		f.Close()

	}

	return nil
}

func (b *Bitcask) appendRecord(key, value string) (KeydirEntry, error) {
	ts := time.Now().UnixNano()
	encodedByte := encodeRecord(key, value, ts)

	currentOffset := b.ActiveFileSize

	n, err := b.activeFileHandle.Write(encodedByte)
	if err != nil {
		slog.Error("failed to write to active data file", "error", err)
		return KeydirEntry{}, err
	}

	if n < len(encodedByte) {
		if err := b.Close(); err != nil {
			slog.Error("failed to close current active data file on partial write detection", "error", err)
		}
		slog.Error("partial write detected while appending to active data file")
		return KeydirEntry{}, fmt.Errorf("partial write detected while appending to active data file")
	}

	b.ActiveFileSize += int64(n)

	entry := KeydirEntry{
		fileID:      b.ActiveFileID,
		valueOffset: currentOffset + 20 + int64(len(key)),
		valueSize:   int64(len(value)),
		timestamp:   ts,
	}

	return entry, nil
}
