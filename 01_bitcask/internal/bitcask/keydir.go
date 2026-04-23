package bitcask

type KeydirEntry struct {
	fileID      string
	valueOffset int64
	valueSize   int64
	timestamp   int64
}

type Keydir map[string]KeydirEntry

func (k Keydir) Set(key string, entry KeydirEntry) {
	k[key] = entry
}

func (k Keydir) Get(key string) (KeydirEntry, bool) {
	entry, ok := k[key]
	return entry, ok
}
