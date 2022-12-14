package indexes

import (
	"github.com/DmitriyVTitov/size"
	"github.com/google/btree"
)

//TODO: write own test bench

type BTreeSecondaryIndex struct {
	baseTree *btree.BTreeG[KeyValue]
	gapSize  uint64
	numKeys  uint64
}

func (B *BTreeSecondaryIndex) Lookup(key uint64) SearchBound {
	// basic idea: since BTree is a secondary index we need to
	// do a range lookup instead of a key Lookup

	// initialized to length of data, can also maybe be len(baseTree) * gapSize?
	var upperBound = &B.numKeys
	iter := func(k KeyValue) bool {
		if k.Key > key {
			*upperBound = k.Value
			return false
			// hit the key, just give it a valid search bound and let it go
		} else if k.Key == key {
			*upperBound = k.Value + 1
			return false
		}
		return true
	}
	B.baseTree.AscendGreaterOrEqual(KeyValue{key, 0}, iter)
	var lower = *upperBound - B.gapSize
	if *upperBound < B.gapSize {
		lower = 0
	}
	return SearchBound{Start: lower, Stop: *upperBound}
}

func (B *BTreeSecondaryIndex) Size() int64 {
	return int64(size.Of(B))
}

func (B *BTreeSecondaryIndex) Name() string {
	return "BTree"
}

func NewBtreeIndex(keyValues *[]KeyValue, gap uint64) SecondaryIndex {
	var tree = btree.NewMap[uint64, uint64](4)
	for i := 0; i < len(*keyValues); i += int(gap) { //load in every gap size element
		var curKeyVal = (*keyValues)[i]
		tree.ReplaceOrInsert(curKeyVal)
	}
	// we need to add the last value so that the b-tree has a complete picture
	lastValue := (*keyValues)[len(*keyValues)-1]
	tree.ReplaceOrInsert(lastValue)
	return &BTreeSecondaryIndex{
		baseTree: tree,
		gapSize:  gap,
		numKeys:  uint64(len(*keyValues)),
	}
}
