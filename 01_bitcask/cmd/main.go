package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/blackmamoth/bitcask/internal/bitcask"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	slog.SetDefault(logger)

	db, err := bitcask.NewBitcask(".", 1)
	if err != nil {
		log.Fatalf("failed to instantiate bitcask instance: %v", err)
	}

	defer db.Close()

	// bitcask.Merge()

	for i := range 15 {
		db.Put(fmt.Sprintf("foo%d", i), fmt.Sprintf("bar%d", i))
	}

	db.Sync()

	// val, ok, _ := bitcask.Get("foo3")
	// slog.Info("retrieved value for key", "key", "foo3", "value", val, "ok", ok)

	// // bitcask.Delete("foo3")

	// val, ok, _ = bitcask.Get("foo3")
	// slog.Info("retrieved value for key", "key", "foo3", "value", val, "ok", ok)
	// keys := bitcask.ListKeys()

	// fmt.Println(keys)

	// count, _ := bitcask.Fold(func(key, value string, acc any) any {
	// 	return acc.(int) + 1
	// }, 0)

	// fmt.Println(count)

	// keys, _ := bitcask.Fold(db, func(key, value string, acc []string) []string {
	// 	acc = append(acc, key)
	// 	return acc
	// }, []string{})

	// fmt.Println(keys)

	// add tests next

}
