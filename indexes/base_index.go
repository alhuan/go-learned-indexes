package indexes

type SecondaryIndex interface {
	Lookup(key int64) SearchBound
	Size() int64
	Name() string
}

type SearchBound struct {
	Start int64
	Stop  int64
}

type KeyValue struct {
	Key   int64
	Value int64
}
