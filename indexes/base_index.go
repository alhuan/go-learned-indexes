package indexes

type SecondaryIndex interface {
	Lookup(key int64) SearchBound
	Size() int64
}

type SearchBound struct {
	Start int64
	Stop  int64
}
