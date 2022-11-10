package indexes

import (
	"github.com/tidwall/btree"
	"unsafe"
)

// note that we should only insert every kth key into this btree because we're using it as a
// secondary index. I forgot how this is supposed to work though
type BTreeSecondaryIndex struct {
	baseTree *btree.Map[uint64, uint64]
	gapSize  uint64
}

func (B *BTreeSecondaryIndex) Lookup(key uint64) SearchBound {
	// basic idea: since BTree is a secondary index we need to
	// do a range lookup instead of a key Lookup
	// this is the basic approach I think this function might need
	// as you can see, it's pretty ugly
	// someone else should validate this
	var upperBound *uint64
	pivot := func(k uint64, v uint64) bool {
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
	btreeMap := btree.Map[uint64, uint64]{}
	btreeMap.Load()
	return &BTreeSecondaryIndex{
		baseTree: &btree.Map[uint64, uint64]{},
	}
}
