package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"sort"
	"strings"
)

func encodeRecord(key, value string, ts int64) []byte {

	klen, vlen := len(key), len(value)

	//      crc  ts  kl  vl   k      v
	total := 4 + 8 + 4 + 4 + klen + vlen
	buf := make([]byte, total)

	binary.LittleEndian.PutUint64(buf[4:], uint64(ts))

	binary.LittleEndian.PutUint32(buf[12:], uint32(klen))
	binary.LittleEndian.PutUint32(buf[16:], uint32(vlen))

	copy(buf[20:], key)
	copy(buf[20+klen:], value)

	crc := crc32.ChecksumIEEE(buf[4:])

	binary.LittleEndian.PutUint32(buf[:4], crc)

	return buf
}

func decodeRecord(buf []byte) (key, value string, ts int64, err error) {
	if len(buf) < 20 {
		return "", "", 0, fmt.Errorf("buffer too small")
	}

	storedCRC := binary.LittleEndian.Uint32(buf[:4])

	computedCRC := crc32.ChecksumIEEE(buf[4:])
	if storedCRC != computedCRC {
		return "", "", 0, fmt.Errorf("crc mismatch")
	}

	ts = int64(binary.LittleEndian.Uint64(buf[4:12]))

	klen := binary.LittleEndian.Uint32(buf[12:16])
	vlen := binary.LittleEndian.Uint32(buf[16:20])

	expectedLen := 20 + int(klen) + int(vlen)
	if len(buf) < expectedLen {
		return "", "", 0, fmt.Errorf("invalid buffer length")
	}

	keyStart := 20
	keyEnd := keyStart + int(klen)
	valEnd := keyEnd + int(vlen)

	key = string(buf[keyStart:keyEnd])
	value = string(buf[keyEnd:valEnd])

	return key, value, ts, nil
}

func writeRecord(
	f *os.File,
	fileID string,
	currentOffset int64,
	ts int64,
	key, value string,
) (KeydirEntry, int64, error) {

	encoded := encodeRecord(key, value, ts)

	n, err := f.Write(encoded)
	if err != nil {
		slog.Error("failed to write record to file", "file_id", fileID, "error", err)
		return KeydirEntry{}, 0, err
	}

	if n < len(encoded) {
		slog.Error("partial write detected", "file_id", fileID)
		return KeydirEntry{}, 0, fmt.Errorf("partial write detected")
	}

	entry := KeydirEntry{
		fileID:      fileID,
		valueOffset: currentOffset + 20 + int64(len(key)),
		valueSize:   int64(len(value)),
		timestamp:   ts,
	}

	return entry, int64(n), nil
}

func listDataFiles(dirPath string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		slog.Error("error reading bitcask directory", "error", err)
		return nil, err
	}

	n := 0

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".data") {
			files[n] = file
			n++
		}
	}

	files = files[:n]

	return files, nil
}

func sortDataFilesByName(files []os.DirEntry) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})
}
