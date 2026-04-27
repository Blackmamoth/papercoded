package bitcask

import "testing"

func TestKeydirSetGet(t *testing.T) {
	tests := []struct {
		key   string
		entry KeydirEntry
	}{
		{
			key: "foo",
			entry: KeydirEntry{
				fileID:      "file123.data",
				valueOffset: 2,
				valueSize:   4,
				timestamp:   123456789,
			},
		},
		{
			key: "foo2",
			entry: KeydirEntry{
				fileID:      "file124.data",
				valueOffset: 8,
				valueSize:   1,
				timestamp:   123456789,
			},
		},
		{
			key: "foo3",
			entry: KeydirEntry{
				fileID:      "file125.data",
				valueOffset: 12,
				valueSize:   4,
				timestamp:   123456789,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			keydir := make(Keydir)

			keydir.Set(tt.key, tt.entry)

			entry, ok := keydir.Get(tt.key)

			if !ok {
				t.Fatalf("expected key %q to exist", tt.key)
			}

			if entry != tt.entry {
				t.Fatalf("entry mismatch for key %q: expected %+v got %+v", tt.key, tt.entry, entry)
			}
		})
	}
}

func TestKeydirGetMissing(t *testing.T) {
	keydir := make(Keydir)

	key := "foo"

	_, ok := keydir.Get(key)

	if ok {
		t.Fatalf("expected key %q to be missing", key)
	}
}

func TestKeydirSetOverwrite(t *testing.T) {
	keydir := make(Keydir)

	key := "foo"

	entryA := KeydirEntry{
		fileID:      "file123",
		valueOffset: 4,
		valueSize:   1,
		timestamp:   123456789,
	}

	entryB := KeydirEntry{
		fileID:      "file432",
		valueOffset: 34,
		valueSize:   6,
		timestamp:   123456789,
	}

	keydir.Set(key, entryA)

	entry, ok := keydir.Get(key)
	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}

	if entry != entryA {
		t.Fatalf("expected key %q to equal to entry A, got %+v", key, entry)
	}

	keydir.Set(key, entryB)

	entry, ok = keydir.Get(key)
	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}

	if entry != entryB {
		t.Fatalf("expected key %q to equal to entry B, got %+v", key, entry)
	}
}
