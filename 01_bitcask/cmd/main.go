package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/blackmamoth/bitcask/internal/bitcask"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	slog.SetDefault(logger)

	bitcask, err := bitcask.NewBitcask(".", 1)
	if err != nil {
		log.Fatalf("failed to instantiate bitcask instance: %v", err)
	}

	defer bitcask.Close()

	// for i := range 10 {
	// 	bitcask.Put(fmt.Sprintf("foo%d", i), fmt.Sprintf("bar%d", i))
	// }

	// val, ok, _ := bitcask.Get("foo3")
	// slog.Info("retrieved value for key", "key", "foo3", "value", val, "ok", ok)

	// bitcask.Delete("foo3")

	val, ok, _ := bitcask.Get("foo3")
	slog.Info("retrieved value for key", "key", "foo3", "value", val, "ok", ok)
}
