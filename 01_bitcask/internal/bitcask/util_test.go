package bitcask

import (
	"testing"
)

func TestEncodeDecodeRecord(t *testing.T) {

	tests := []struct {
		name      string
		key       string
		value     string
		timestamp int64
	}{
		{
			name: "simple ascii", key: "foo", value: "bar", timestamp: 123456789,
		},
		{
			name: "empty value", key: "foo1", value: "", timestamp: 123456789,
		},
		{
			name: "empty key", key: "", value: "bar3", timestamp: 123456789,
		},
		{
			name: "unicode", key: "ключ", value: "नमस्ते 🚀", timestamp: 123456789,
		},
		{
			name: "zero timestamp", key: "foo", value: "bar", timestamp: 0,
		},
		{
			name: "negative timestamp", key: "foo", value: "bar", timestamp: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeRecord(tt.key, tt.value, tt.timestamp)

			expectedLen := 20 + len(tt.key) + len(tt.value)

			if len(encoded) != expectedLen {
				t.Fatalf("length of encoded version does not match expected length, expected %d, got %d", expectedLen, len(encoded))
			}

			k, v, ts, err := decodeRecord(encoded)

			if err != nil {
				t.Fatal(err)
			}

			if k != tt.key {
				t.Fatalf("decoded key does not match the encoded version. expected key to be %s, got %s", tt.key, k)
			}

			if v != tt.value {
				t.Fatalf("decoded value does not match the encoded version. expected value to be %s. got %s", tt.value, v)
			}

			if ts != tt.timestamp {
				t.Fatalf("decoded timestamp does not match the encoded version. expected timestamp to be %d, got %d", tt.timestamp, ts)
			}
		})

	}

}

func TestDecodeRecordErrors(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
	}{
		{
			name: "buffer too small",
			buf:  []byte{1, 2, 3},
		},
		{
			name: "crc mismatch",
			buf: func() []byte {
				buf := encodeRecord("foo", "bar", 123)
				buf[4]++
				return buf
			}(),
		},
		{
			name: "truncated value",
			buf: func() []byte {
				buf := encodeRecord("foo", "bar", 123)
				return buf[:len(buf)-1]
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := decodeRecord(tt.buf)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}
