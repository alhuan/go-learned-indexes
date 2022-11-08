package indexes

import (
	"github.com/tidwall/btree"
	"unsafe"
)

// note that we should only insert every kth key into this btree because we're using it as a
// secondary index. I forgot how this is supposed to work though
type BTreeSecondaryIndex struct {
	baseTree *btree.Map[int64, int64]
	gapSize int64
}

func (B *BTreeSecondaryIndex) Lookup(key int64) SearchBound {
	// basic idea: since BTree is a secondary index we need to
	// do a range lookup instead of a key Lookup
	// this is the basic approach I think this function might need
	// as you can see, it's pretty ugly
	// someone else should validate this
	var upperBound *int64
	pivot := func(k int64, v int64) bool {
		if k >= key {
			*upperBound = v
			return false
		}
		return true
	}
	B.baseTree.Ascend(key, pivot)
	// this needs to be clamped to [0, range_max]
	return SearchBound{Start: *upperBound - B.gapSize, Stop: *upperBound}
}

func (B *BTreeSecondaryIndex) Size() int64 {
	return int64(unsafe.Sizeof(B))
}

func (B *BTreeSecondaryIndex) Name() string {
	return "BTree"
}

func NewBtreeIndex(keyValues *[]KeyValue) SecondaryIndex {
	btreeMap := btree.Map[int64, int64]{}
	btreeMap.Load()
	return &BTreeSecondaryIndex{
		baseTree: &btree.Map[int64, int64]{},
	}
}
