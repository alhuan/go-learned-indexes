package indexes

import (
	"github.com/tidwall/btree"
	"unsafe"
)

//TODO: write own test bench

type BTreeSecondaryIndex struct {
	baseTree *btree.Map[uint64, uint64]
	gapSize  uint64
	numKeys  uint64
}

func (B *BTreeSecondaryIndex) Lookup(key uint64) SearchBound {
	// basic idea: since BTree is a secondary index we need to
	// do a range lookup instead of a key Lookup

	// initialized to length of data, can also maybe be len(baseTree) * gapSize?
	var upperBound = &B.numKeys
	pivot := func(k uint64, v uint64) bool {
		if k > key {
			*upperBound = v
			return false
		}
		return true
	}
	B.baseTree.Ascend(key, pivot)
	var lower = *upperBound - B.gapSize
	if lower < 0 {
		lower = 0
	}
	return SearchBound{Start: lower, Stop: *upperBound}
}

func (B *BTreeSecondaryIndex) Size() int64 {
	return int64(unsafe.Sizeof(B))
}

func (B *BTreeSecondaryIndex) Name() string {
	return "BTree"
}

func NewBtreeIndex(keyValues *[]KeyValue, gap uint64) SecondaryIndex {
	var tree = &btree.Map[uint64, uint64]{}
	for i := 0; i < len(*keyValues); i += int(gap) { //load in every gap size element
		var curKeyVal = (*keyValues)[i]
		tree.Load(curKeyVal.Key, curKeyVal.Value)
	}
	return &BTreeSecondaryIndex{
		baseTree: tree,
		gapSize:  gap,
		numKeys:  uint64(len(*keyValues)),
	}
}
