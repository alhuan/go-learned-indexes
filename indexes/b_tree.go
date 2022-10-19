package indexes

import (
	"github.com/tidwall/btree"
)

// note that we should only insert every kth key into this btree because we're using it as a
// secondary index. I forgot how this is supposed to work though
type BTreeSecondaryIndex struct {
	baseTree *btree.Map[int64, int64]
}

func (B *BTreeSecondaryIndex) Lookup(key int64) SearchBound {
	//TODO implement me
	panic("implement me")
}

func (B *BTreeSecondaryIndex) Size() int64 {
	//TODO implement me
	panic("implement me")
}

func NewBtreeIndex(keyValues *[]KeyValue) SecondaryIndex {
	return &BTreeSecondaryIndex{
		baseTree: &btree.Map[int64, int64]{},
	}
}
