package bitcask

import (
	"fmt"
	"maps"
	"slices"
	"sync"
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

func TestBitcaskMerge(t *testing.T) {
	pairs := []struct {
		key   string
		value string
	}{
		{key: "foo", value: "bar-old"},
		{key: "foo1", value: "bar1-old"},
		{key: "foo2", value: "bar2-old"},
		{key: "foo3", value: "bar3-old"},
		{key: "foo4", value: "bar4-old"},
		{key: "foo5", value: "bar5-old"},
		{key: "foo6", value: "bar6-old"},
		{key: "foo7", value: "bar7-old"},
		{key: "foo8", value: "bar8-old"},
		{key: "foo9", value: "bar9-old"},
		{key: "foo", value: "bar-new"},
		{key: "foo1", value: "bar1-new"},
		{key: "foo2", value: "bar2-new"},
		{key: "foo3", value: "bar3-new"},
		{key: "foo4", value: "bar4-new"},
		{key: "foo5", value: "bar5-new"},
		{key: "foo6", value: "bar6-new"},
		{key: "foo7", value: "bar7-new"},
		{key: "foo8", value: "bar8-new"},
		{key: "foo9", value: "bar9-new"},
	}

	dir := t.TempDir()

	db, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, entry := range pairs {
		err = db.Put(entry.key, entry.value)
		if err != nil {
			t.Fatal(err)
		}
	}

	filesBeforeMerge, err := listDataFiles(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(filesBeforeMerge) != len(pairs) {
		t.Fatalf("length mismatch for no. of data files, expected %d, got %d", len(pairs), len(filesBeforeMerge))
	}

	err = db.Merge()
	if err != nil {
		t.Fatal(err)
	}

	filesAfterMerge, err := listDataFiles(dir)
	if err != nil {
		t.Fatal(err)
	}

	if !(len(filesBeforeMerge) > len(filesAfterMerge)) {
		t.Fatalf("expected length to be less then before merge, expected < %d, got %d", len(filesBeforeMerge), len(filesAfterMerge))
	}

	testPairs := pairs[len(pairs)/2:]

	for _, tt := range testPairs {
		t.Run(tt.key, func(t *testing.T) {
			value, ok, err := db.Get(tt.key)
			if err != nil {
				t.Fatal(err)
			}

			if !ok {
				t.Fatalf("expected key %q to exist after merge", tt.key)
			}

			if value != tt.value {
				t.Fatalf("expected key %q to have newer value after merge, expected %q, got %q", tt.key, tt.value, value)
			}
		})
	}

	newKey, newValue := "foo10", "bar10-new"

	err = db.Put(newKey, newValue)
	if err != nil {
		t.Fatal(err)
	}

	value, ok, err := db.Get(newKey)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected newly added key %q after merge, to exist", newKey)
	}

	if value != newValue {
		t.Fatalf("expected value of newly added key after merge to be %q, got %q", newValue, value)
	}

}

func TestBitcaskMergeRecovery(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo1", "bar1")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo2", "bar2")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo1", "bar1_updated")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Delete("foo2")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Merge()
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db2, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	value, ok, err := db2.Get("foo1")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", "foo")
	}

	if value != "bar1_updated" {
		t.Fatalf("expected key %q, to have value %q, got %q", "foo", "bar1_updated", value)
	}

	_, ok, err = db2.Get("foo2")
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatalf("expected key %q to be missing", "foo2")
	}
}

func TestBitcaskRecoveryPut(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db2, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	value, ok, err := db2.Get("foo")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", "foo")
	}

	if value != "bar" {
		t.Fatalf("expected key %q to have value %q, got %q", "foo", "bar", value)
	}
}

func TestBitcaskRecoveryDelete(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo2", "bar2")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Delete("foo")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db2, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	_, ok, err := db2.Get("foo")
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatalf("expected key %q to be missing", "foo")
	}

	value, ok, err := db2.Get("foo2")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", "foo2")
	}

	if value != "bar2" {
		t.Fatalf("expected key %q to have value %q, got %q", "foo2", "bar2", value)
	}
}

func TestBitcaskRecoveryUpdate(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo", "baz")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db2, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	value, ok, err := db2.Get("foo")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", "foo")
	}

	if value != "baz" {
		t.Fatalf("expected key %q to have value %q, got %q", "foo", "baz", value)
	}
}

func TestBitcaskRecoveryMultipleFiles(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}

	pairs := map[string]string{
		"foo":  "bar",
		"foo1": "bar1",
		"foo2": "bar2",
	}

	for key, value := range pairs {
		err = db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Close()

	db2, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for key, expectedValue := range pairs {
		t.Run(key, func(t *testing.T) {
			value, ok, err := db2.Get(key)
			if err != nil {
				t.Fatal(err)
			}

			if !ok {
				t.Fatalf("expected key %q to exist", key)
			}

			if value != expectedValue {
				t.Fatalf("expected key %q to have value %q, got %q", key, expectedValue, value)
			}
		})
	}
}

func TestBitcaskRecoveryUpdateAcrossFiles(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo", "baz")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db2, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	value, ok, err := db2.Get("foo")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", "foo")
	}

	if value != "baz" {
		t.Fatalf("expected key %q to have value %q, got %q", "foo", "baz", value)
	}
}

func TestBitcaskRecoveryDeleteAcrossFiles(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Delete("foo")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db2, err := NewBitcask(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	_, ok, err := db2.Get("foo")
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatalf("expected key %q to be missing", "foo")
	}
}

func TestBitcaskConcurrentPuts(t *testing.T) {
	dir := t.TempDir()
	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup

	for i := range 10 {
		wg.Go(func() {
			key := fmt.Sprintf("foo%d", i)
			value := fmt.Sprintf("bar%d", i)

			err := db.Put(key, value)
			if err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	for i := range 10 {
		wg.Go(func() {
			key := fmt.Sprintf("foo%d", i)
			expectedValue := fmt.Sprintf("bar%d", i)

			value, ok, err := db.Get(key)
			if err != nil {
				t.Error(err)
			}

			if !ok {
				t.Errorf("expected key %q to exist", key)
			}

			if value != expectedValue {
				t.Errorf("expected value for key %q to be %q, got %q", key, expectedValue, value)
			}
		})
	}

	wg.Wait()
}

func TestBitcaskConcurrentPutGetSameKey(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "foo"
	writtenValues := map[string]bool{}

	var wg sync.WaitGroup

	for i := range 10 {
		value := fmt.Sprintf("bar%d", i)
		writtenValues[value] = true

		wg.Go(func() {
			err := db.Put(key, value)
			if err != nil {
				t.Error(err)
			}

			_, _, err = db.Get(key)
			if err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	value, ok, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}

	if !writtenValues[value] {
		t.Fatalf("expected final value to be one of %+v, got %q", writtenValues, value)
	}
}

func TestBitcaskConcurrentListKeysAndPut(t *testing.T) {
	dir := t.TempDir()

	db, err := NewBitcask(dir, 1*MB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			key := fmt.Sprintf("foo%d", i)
			value := fmt.Sprintf("bar%d", i)

			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Put returned error: %v", err)
			}
		})

		wg.Go(func() {
			_ = db.ListKeys()
		})
	}

	wg.Wait()

	keys := db.ListKeys()

	if len(keys) != 10 {
		t.Fatalf("expeceted 10 keys, got %d: %v", len(keys), keys)
	}

}
