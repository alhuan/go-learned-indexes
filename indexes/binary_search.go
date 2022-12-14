package indexes

type BinarySearch struct {
	len uint64
}

func (b *BinarySearch) Lookup(key uint64) SearchBound {
	return SearchBound{0, b.len}
}

func (b *BinarySearch) Size() int64 {
	return 0
}

func (b *BinarySearch) Name() string {
	return "BinarySearch"
}

func NewBinarySearch(data *[]KeyValue) SecondaryIndex {
	return &BinarySearch{len: uint64(len(*data))}
}
