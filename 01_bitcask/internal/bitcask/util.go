package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
