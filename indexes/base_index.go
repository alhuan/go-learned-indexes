package indexes

type SecondaryIndex interface {
	Lookup(key uint64) SearchBound
	Size() int64
	Name() string
}

type SearchBound struct {
	Start uint64
	Stop  uint64
}

type KeyValue struct {
	Key   uint64
	Value uint64
}
