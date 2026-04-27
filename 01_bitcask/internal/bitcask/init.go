package bitcask

import (
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	KB int64 = 1024
	MB       = 1024 * KB
	GB       = 1024 * MB
)

type Bitcask struct {
	dirPath          string
	maxFileSize      int64
	counter          uint64
	mutex            sync.RWMutex
	once             sync.Once
	activeFileHandle *os.File
	keydir           Keydir
	ActiveFileID     string
	ActiveFileSize   int64
}

func NewBitcask(dirPath string, maxFileSize int64) (*Bitcask, error) {

	db := &Bitcask{
		dirPath:     dirPath,
		maxFileSize: maxFileSize,
		keydir:      make(Keydir),
	}

	if err := db.Open(); err != nil {
		return nil, err
	}

	// slog.Info("Bitcask instance opened")

	return db, nil
}

func (b *Bitcask) Open() error {

	var openErr error = nil

	b.once.Do(func() {

		if err := b.ensureDirectoryExists(); err != nil {
			openErr = err
			return
		}

		files, err := listDataFiles(b.dirPath)
		if err != nil {
			// slog.Error("failed to list data files from bitcask directory", "error", err)
			openErr = err
			return
		}

		if len(files) == 0 {
			b.counter = 0
			b.activeFileHandle = nil
			b.ActiveFileID = ""
			b.ActiveFileSize = 0
		} else {

			if err := b.initializeKeydir(files); err != nil {
				// slog.Error("failed to initialize keydir", "error", err)
				openErr = err
				return
			}

			sortDataFilesByName(files)

			latestFile := files[len(files)-1]

			newCounter, err := b.getCountFromFileName(latestFile.Name())
			if err != nil {
				// slog.Error("failed to get latest counter for file", "file_name", latestFile.Name(), "error", err)
				openErr = err
				return
			}

			b.counter = newCounter
			b.activeFileHandle = nil
			b.ActiveFileID = ""
			b.ActiveFileSize = 0
		}
	})

	return openErr
}

func (b *Bitcask) Put(key, value string) error {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	entry, err := b.appendRecord(key, value)

	if err != nil {
		// slog.Error("failed to append record to current active data file", "key", key, "value", value, "active_file_id", b.ActiveFileID)
		return fmt.Errorf("failed to append record for key %s: %v", key, err)
	}

	b.keydir.Set(key, entry)

	// slog.Info("set key in keydir", "key", key)

	return nil
}

func (b *Bitcask) Get(key string) (string, bool, error) {

	b.mutex.RLock()
	keydirEntry, ok := b.keydir.Get(key)
	b.mutex.RUnlock()
	if !ok {
		// slog.Warn("attempted to GET non-existent key", "key", key)
		return "", false, nil
	}

	fileID := keydirEntry.fileID

	f, err := os.Open(filepath.Join(b.dirPath, fileID))
	if err != nil {
		// slog.Error("failed to open data file for reading value", "file_id", fileID, "error", err)
		return "", false, err
	}
	defer f.Close()

	valueBuf := make([]byte, keydirEntry.valueSize)

	_, err = f.Seek(keydirEntry.valueOffset, io.SeekStart)
	if err != nil {
		// slog.Error("failed to seek to value offset in data file", "file_id", fileID, "offset", keydirEntry.valueOffset, "error", err)
		return "", false, fmt.Errorf("seek failed for file %s at offset %d: %w", fileID, keydirEntry.valueOffset, err)
	}

	_, err = io.ReadFull(f, valueBuf)
	if err != nil {
		// slog.Error("failed to read value buffer from data file", "file_id", fileID, "error", err)
		return "", false, err
	}

	return string(valueBuf), true, nil
}

func (b *Bitcask) Delete(key string) error {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	_, ok := b.keydir.Get(key)
	if !ok {
		// slog.Error("attempted to delete non-existent key", "key", key)
		return fmt.Errorf("attempated to delete non-existent key [%s]", key)
	}

	_, err := b.appendRecord(key, "")

	if err != nil {
		// slog.Error("failed to append record to current active data file", "key", key, "value", "<EMPTY>", "active_file_id", b.ActiveFileID)
		return fmt.Errorf("failed to append record for key %s: %v", key, err)
	}

	delete(b.keydir, key)

	// slog.Info("DELETE key from keydir", "key", key)

	return nil
}

func (b *Bitcask) ListKeys() []string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	keys := make([]string, 0, len(b.keydir))

	for k := range b.keydir {
		keys = append(keys, k)
	}

	return keys
}

func (b *Bitcask) fold(fn func(key, value string, acc any) any, initial any) (any, error) {
	b.mutex.RLock()
	keydirSnapshot := make(Keydir)
	maps.Copy(keydirSnapshot, b.keydir)
	b.mutex.RUnlock()

	accumulator := initial
	openFileHandles := map[string]*os.File{}

	defer func() {
		for _, handle := range openFileHandles {
			handle.Close()
		}
	}()

	for k, entry := range keydirSnapshot {

		fileID := entry.fileID

		fileHandle, handleExists := openFileHandles[fileID]
		if !handleExists {
			f, err := os.Open(filepath.Join(b.dirPath, fileID))
			if err != nil {
				// slog.Error("failed to open data file for reading value", "file_id", fileID, "error", err)
				return nil, err
			}

			openFileHandles[fileID] = f
			fileHandle = f
		}

		valueBuf := make([]byte, entry.valueSize)

		_, err := fileHandle.Seek(entry.valueOffset, io.SeekStart)
		if err != nil {
			// slog.Error("failed to seek to value offset in data file", "file_id", fileID, "offset", entry.valueOffset, "error", err)
			return nil, fmt.Errorf("seek failed for file %s at offset %d: %w", fileID, entry.valueOffset, err)
		}

		_, err = io.ReadFull(fileHandle, valueBuf)
		if err != nil {
			// slog.Error("failed to read value buffer from data file", "file_id", fileID, "error", err)
			return nil, err
		}

		val := string(valueBuf)

		accumulator = fn(k, val, accumulator)
	}

	return accumulator, nil
}

func Fold[T any](b *Bitcask, fn func(key, value string, acc T) T, initial T) (T, error) {
	acc := initial

	result, err := b.fold(func(key, value string, acc any) any {
		return fn(key, value, acc.(T))
	}, acc)

	if err != nil {
		return acc, err
	}

	return result.(T), err
}

func (b *Bitcask) Merge() error {
	b.mutex.Lock()
	keydirSnapshot := make(Keydir)
	maps.Copy(keydirSnapshot, b.keydir)
	activeID := b.ActiveFileID
	counter := b.counter
	b.mutex.Unlock()

	newKeyDir, newMergeFiles, err := b.compactSnapshot(keydirSnapshot, activeID, counter)
	if err != nil {
		// slog.Error("failed to compact snapshot during merge", "error", err)
		return err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	for k, entry := range b.keydir {
		if _, isMergeFile := newMergeFiles[entry.fileID]; !isMergeFile {
			newKeyDir[k] = entry
		}
	}

	for k := range keydirSnapshot {
		if _, stillExists := b.keydir[k]; !stillExists {
			delete(newKeyDir, k)
		}
	}

	allDataFiles, err := listDataFiles(b.dirPath)
	if err == nil {
		for _, file := range allDataFiles {
			name := file.Name()
			if name == b.ActiveFileID {
				continue
			}
			if _, ok := newMergeFiles[name]; ok {
				continue
			}
			_ = os.Remove(filepath.Join(b.dirPath, name))
		}
	}

	b.keydir = newKeyDir
	newCounter := counter + uint64(len(newMergeFiles))
	if newCounter > b.counter {
		b.counter = newCounter
	}

	return nil
}

func (b *Bitcask) Sync() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.activeFileHandle == nil {
		return nil
	}

	return b.activeFileHandle.Sync()
}

func (b *Bitcask) Close() error {
	if b.activeFileHandle == nil {
		return nil
	}

	if err := b.activeFileHandle.Sync(); err != nil {
		// slog.Error("failed to sync current active data file", "error", err)
		return err
	}

	if err := b.activeFileHandle.Close(); err != nil {
		// slog.Error("failed to close current active data file", "error", err)
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
				// slog.Error("failed to create bitcask directory", "error", err)
				return err
			}
		} else {
			// slog.Error("failed to retrieve stats for bitcask directory", "error", err)
			return err
		}
	} else if !info.IsDir() {
		// slog.Error("provided path is not a directory")
		return fmt.Errorf("provided path is not a directory")
	}

	return nil
}

func (b *Bitcask) createNewActiveFile() error {

	if err := b.ensureDirectoryExists(); err != nil {
		// slog.Error("failed to ensure bitcask directory exists", "error", err)
		return err
	}

	b.counter++

	var fileName string

	for {
		fileName = fmt.Sprintf("%020d.data", b.counter)
		_, err := os.Stat(filepath.Join(b.dirPath, fileName))
		if os.IsNotExist(err) {
			break
		}
		b.counter++
	}

	file, err := os.OpenFile(filepath.Join(b.dirPath, fileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// slog.Error("failed to create new file in bitcask directory", "error", err)
		return err
	}

	b.activeFileHandle = file
	b.ActiveFileID = fileName
	b.ActiveFileSize = 0

	return nil
}

func (b *Bitcask) getCountFromFileName(fileName string) (uint64, error) {

	name := strings.TrimSuffix(fileName, ".data")

	id, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		// slog.Error("failed to parse uint file id", "error", err)
		return 0, err
	}

	return id, nil
}

func (b *Bitcask) initializeKeydir(files []os.DirEntry) error {

	for _, file := range files {
		var offset int64 = 0
		hintFileExists := false
		hintFileReadSuccessful := false

		hintFileName := strings.Replace(file.Name(), ".data", ".hint", 1)

		hintFileStat, err := os.Stat(filepath.Join(b.dirPath, hintFileName))
		if err != nil {
			if os.IsNotExist(err) {
				// slog.Info("hint file does not exist for a data file, skipping", "data_file", file.Name())
			} else {
				// slog.Error("failed to read hint file stats")
			}
		} else if !hintFileStat.IsDir() {
			hintFileExists = true
		}

		if hintFileExists {
			f, err := os.Open(filepath.Join(b.dirPath, hintFileName))
			if err != nil {
				// slog.Error("failed to read a hint file while initializing keydir", "file_name", hintFileName, "error", err)
				return err
			}

			for {
				key, entry, err := readHintEntry(f, file.Name())
				if err == io.EOF {
					hintFileReadSuccessful = true
					break
				}

				if err != nil {
					// slog.Error("failed to read hint entry", "file_name", hintFileName, "error", err)
					break
				}

				if entry.valueSize == 0 {
					delete(b.keydir, key)
				} else {
					b.keydir.Set(key, entry)
					// slog.Info("setting entry to keydir from hint file", "key", key, "file_name", hintFileName)
				}
			}

			f.Close()
		}

		if !hintFileExists || !hintFileReadSuccessful {
			f, err := os.Open(filepath.Join(b.dirPath, file.Name()))
			if err != nil {
				// slog.Error("failed to read a data file while initializing keydir", "file_name", file.Name(), "error", err)
				return err
			}

			header := make([]byte, 20)
			for {
				_, err := io.ReadFull(f, header)
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}

				if err != nil {
					// slog.Error("failed to read current buffer in data file", "file_name", file.Name(), "error", err)
					return err
				}

				klen := int64(binary.LittleEndian.Uint32(header[12:16]))
				vlen := int64(binary.LittleEndian.Uint32(header[16:20]))

				keyBuf := make([]byte, klen)
				if _, err := io.ReadFull(f, keyBuf); err != nil {
					// slog.Error("failed to read key buffer", "file_name", file.Name(), "error", err)
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

				// slog.Info("setting entry to keydir from data file", "key", key, "file_name", file.Name())

				if _, err := f.Seek(vlen, io.SeekCurrent); err != nil {
					// slog.Error("failed to skip value offset", "file_name", file.Name(), "error", err)
					return err
				}

				offset += 20 + klen + vlen

			}
			f.Close()
		}

	}

	return nil
}

func (b *Bitcask) appendRecord(key, value string) (KeydirEntry, error) {
	if b.activeFileHandle == nil {
		if err := b.createNewActiveFile(); err != nil {
			// slog.Error("failed to create an active data file in bitcask directory", "error", err)
			return KeydirEntry{}, err
		}
	}

	recordSize := int64(20 + len(key) + len(value))

	if b.ActiveFileSize+recordSize > b.maxFileSize && b.ActiveFileSize != 0 {
		if err := b.activeFileHandle.Sync(); err != nil {
			// slog.Error("failed to sync actie data file", "error", err)
			return KeydirEntry{}, err
		}

		if err := b.activeFileHandle.Close(); err != nil {
			// slog.Error("failed to close active data file", "error", err)
			return KeydirEntry{}, err
		}

		b.activeFileHandle = nil

		if err := b.createNewActiveFile(); err != nil {
			// slog.Error("failed to create an active data file in bitcask directory", "error", err)
			return KeydirEntry{}, err
		}
	}

	ts := time.Now().UnixNano()
	entry, written, err := writeRecord(b.activeFileHandle, b.ActiveFileID, b.ActiveFileSize, ts, key, value)

	if err != nil {
		// slog.Error("failed to append record to data file", "file_name", b.ActiveFileID, "error", err)
		return KeydirEntry{}, err
	}

	b.ActiveFileSize += written

	return entry, nil
}

func writeHintEntry(hintFileHandle *os.File, key string, entry KeydirEntry) error {

	keyBytes := []byte(key)
	keySize := uint32(len(keyBytes))

	//           k      klen       vlen voffset ts
	totalSize := 4 + len(keyBytes) + 8 + 8 + 8
	buf := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(buf[0:4], keySize)

	copy(buf[4:4+len(keyBytes)], keyBytes)

	offset := 4 + len(keyBytes)

	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(entry.valueSize))
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(entry.valueOffset))
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(entry.timestamp))

	n, err := hintFileHandle.Write(buf)
	if err != nil {
		// slog.Error("failed to write hint entry", "error", err)
		return err
	}

	if n < len(buf) {
		// slog.Error("partial write detected in hint file")
		return fmt.Errorf("partial write in hint file")
	}

	return nil
}

func readHintEntry(r io.Reader, fileID string) (string, KeydirEntry, error) {
	var keySize uint32

	if err := binary.Read(r, binary.LittleEndian, &keySize); err != nil {
		return "", KeydirEntry{}, err
	}

	keyBuf := make([]byte, keySize)
	if _, err := io.ReadFull(r, keyBuf); err != nil {
		return "", KeydirEntry{}, err
	}

	var valueSize uint64
	if err := binary.Read(r, binary.LittleEndian, &valueSize); err != nil {
		return "", KeydirEntry{}, err
	}

	var valueOffset uint64
	if err := binary.Read(r, binary.LittleEndian, &valueOffset); err != nil {
		return "", KeydirEntry{}, err
	}

	var timestamp uint64
	if err := binary.Read(r, binary.LittleEndian, &timestamp); err != nil {
		return "", KeydirEntry{}, err
	}

	entry := KeydirEntry{
		fileID:      fileID,
		valueOffset: int64(valueOffset),
		valueSize:   int64(valueSize),
		timestamp:   int64(timestamp),
	}

	return string(keyBuf), entry, nil
}

func (b *Bitcask) compactSnapshot(snapshot Keydir, activeID string, counter uint64) (Keydir, map[string]struct{}, error) {
	openFileHandles := make(map[string]*os.File)
	newKeyDir := make(Keydir)
	newMergeFiles := make(map[string]struct{})

	latestCounter := counter + 1

	var mergeFileHandle *os.File
	var hintFileHandle *os.File
	var currentMergeFileName string
	var currentHintFileName string
	var currentMergeFileSize int64

	defer func() {
		for _, fh := range openFileHandles {
			fh.Sync()
			fh.Close()
		}

		if mergeFileHandle != nil {
			mergeFileHandle.Sync()
			mergeFileHandle.Close()
		}

		if hintFileHandle != nil {
			hintFileHandle.Sync()
			hintFileHandle.Close()
		}
	}()

	for key, entry := range snapshot {
		if entry.fileID == activeID {
			newKeyDir.Set(key, entry)
			continue
		}

		fileHandle, ok := openFileHandles[entry.fileID]
		if !ok {
			fh, err := os.Open(filepath.Join(b.dirPath, entry.fileID))
			if err != nil {
				return nil, nil, err
			}
			fileHandle = fh
			openFileHandles[entry.fileID] = fileHandle
		}

		valueBuf := make([]byte, entry.valueSize)

		if _, err := fileHandle.Seek(entry.valueOffset, io.SeekStart); err != nil {
			return nil, nil, err
		}

		if _, err := io.ReadFull(fileHandle, valueBuf); err != nil {
			return nil, nil, err
		}

		if mergeFileHandle == nil || currentMergeFileSize >= b.maxFileSize {
			if mergeFileHandle != nil {
				mergeFileHandle.Sync()
				mergeFileHandle.Close()
				mergeFileHandle = nil
			}

			if hintFileHandle != nil {
				hintFileHandle.Sync()
				hintFileHandle.Close()
				hintFileHandle = nil
			}

			currentMergeFileName = fmt.Sprintf("%020d.data", latestCounter)

			fh, err := os.OpenFile(
				filepath.Join(b.dirPath, currentMergeFileName),
				os.O_APPEND|os.O_CREATE|os.O_WRONLY,
				0644,
			)
			if err != nil {
				// slog.Error("failed to create merge file handle", "file_name", currentMergeFileName, "error", err)
				return nil, nil, err
			}

			currentHintFileName = strings.Replace(currentMergeFileName, ".data", ".hint", 1)

			hfh, err := os.OpenFile(
				filepath.Join(b.dirPath, currentHintFileName),
				os.O_APPEND|os.O_CREATE|os.O_WRONLY,
				0644,
			)
			if err != nil {
				// slog.Error("failed to create hint file handle", "file_name", currentHintFileName, "error", err)
				return nil, nil, err
			}

			mergeFileHandle = fh
			hintFileHandle = hfh
			newMergeFiles[currentMergeFileName] = struct{}{}
			currentMergeFileSize = 0
			latestCounter++
		}

		newEntry, written, err := writeRecord(
			mergeFileHandle,
			currentMergeFileName,
			currentMergeFileSize,
			entry.timestamp,
			key,
			string(valueBuf),
		)
		if err != nil {
			return nil, nil, err
		}

		currentMergeFileSize += written
		newKeyDir.Set(key, newEntry)

		if err := writeHintEntry(hintFileHandle, key, newEntry); err != nil {
			// slog.Error("failed to write to hint file", "file_name", currentHintFileName, "error", err)
			return nil, nil, err
		}
	}

	return newKeyDir, newMergeFiles, nil
}
