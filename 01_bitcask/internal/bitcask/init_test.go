package bitcask

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

func TestBitcaskPutGet(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key, value := "foo", "bar"

	err = db.Put(key, value)
	if err != nil {
		t.Fatal(err)
	}

	v, ok, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}

	if v != value {
		t.Fatalf("expected value for key %q to be %q, got %q", key, value, v)
	}
}

func TestBitcaskPut(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "foo"

	val1 := "bar1"
	val2 := "bar2"

	err = db.Put("", "value")
	if err == nil {
		t.Fatalf("expected error for empty key, got nil")
	}

	err = db.Put(key, val1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put(key, val2)
	if err != nil {
		t.Fatal(err)
	}

	value, ok, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}

	if value != val2 {
		t.Fatalf("expected value to be %q, got %q", val2, value)
	}

}

func TestBitcaskGet(t *testing.T) {
	dir := t.TempDir()
	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "foo"

	v, ok, err := db.Get(key)

	if err != nil {
		t.Fatalf("expected error to be nil, got %v", err)
	}

	if ok {
		t.Fatalf("expected key %q to be missing", key)
	}

	if v != "" {
		t.Fatalf("expected value for key %q to be empty, got %q", key, v)
	}

}

func TestBitcaskDelete(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key, value := "foo", "bar"

	err = db.Put(key, value)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	val, ok, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatalf("expected deleted key %q to be missing", key)
	}

	if val != "" {
		t.Fatalf("expected value for deleted key %q to be empty, got %q", key, val)
	}

	err = db.Delete("foo2")
	if err == nil {
		t.Fatalf("expected error, got nil when deleting a non-existent key")
	}
}

func assertKeys(t *testing.T, got []string, expected map[string]string) {
	t.Helper()

	if len(got) != len(expected) {
		t.Fatalf("ListKeys length: expected %d, got %d: %v", len(expected), len(got), got)
	}

	for _, key := range got {
		if _, exists := expected[key]; !exists {
			t.Fatalf("ListKeys returned unexpected key %q; got %v", key, got)
		}
	}

	for key := range expected {
		if !slices.Contains(got, key) {
			t.Fatalf("ListKeys missing key %q; got %v", key, got)
		}
	}
}

func TestBitcaskListKeys(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := map[string]string{
		"foo":  "bar",
		"baz":  "qax",
		"fred": "plugh",
	}

	for key, value := range pairs {
		err = db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	listKeys := db.ListKeys()
	assertKeys(t, listKeys, pairs)

	err = db.Delete("baz")
	if err != nil {
		t.Fatal(err)
	}
	delete(pairs, "baz")

	listKeys = db.ListKeys()
	assertKeys(t, listKeys, pairs)
}

func TestBitcaskFold(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := map[string]string{
		"foo":  "bar",
		"baz":  "qax",
		"fred": "plugh",
	}

	for key, value := range pairs {
		err = db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	keyValPairs, err := Fold[map[string]string](db, func(key, value string, acc map[string]string) map[string]string {
		acc[key] = value
		return acc
	}, map[string]string{})

	if err != nil {
		t.Fatal(err)
	}

	if !maps.Equal(keyValPairs, pairs) {
		t.Fatalf("fold result mismatch: expected %+v, got %+v", pairs, keyValPairs)
	}

	err = db.Delete("baz")
	if err != nil {
		t.Fatal(err)
	}
	delete(pairs, "baz")

	keyValPairs, err = Fold[map[string]string](db, func(key, value string, acc map[string]string) map[string]string {
		acc[key] = value
		return acc
	}, map[string]string{})

	if err != nil {
		t.Fatal(err)
	}

	if !maps.Equal(keyValPairs, pairs) {
		t.Fatalf("fold after delete mismatch: expected %+v, got %+v", pairs, keyValPairs)
	}
}

func TestBitcaskFileRotation(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		key          string
		value        string
		expectedFile string
	}{
		{
			key:          "foo",
			value:        "bar",
			expectedFile: fmt.Sprintf("%020d.data", 1),
		},
		{
			key:          "foo1",
			value:        "bar1",
			expectedFile: fmt.Sprintf("%020d.data", 2),
		},
		{
			key:          "foo2",
			value:        "bar2",
			expectedFile: fmt.Sprintf("%020d.data", 3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			err = db.Put(tt.key, tt.value)
			if err != nil {
				t.Fatal(err)
			}

			entry, ok := db.keydir.Get(tt.key)
			if !ok {
				t.Fatalf("expected key %q to exist in keydir", tt.key)
			}

			if entry.fileID != tt.expectedFile {
				t.Fatalf("expected data to be written in %s, got %s", tt.expectedFile, entry.fileID)
			}

			value, ok, err := db.Get(tt.key)
			if err != nil {
				t.Fatal(err)
			}

			if !ok {
				t.Fatalf("expected key %q to exist", tt.key)
			}

			if value != tt.value {
				t.Fatalf("value mismatch for key %q, expected %q, got %q", tt.key, tt.value, value)
			}

		})
	}

	files, err := listDataFiles(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != len(tests) {
		t.Fatalf("expected no. of data files in bitcask directory to be %d, got %d", len(tests), len(files))
	}

	if db.counter != uint64(len(tests)) {
		t.Fatalf("expected counter to be %d, got %d", len(tests), db.counter)
	}
}
