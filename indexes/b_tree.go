package indexes

import (
	"github.com/google/btree"
)

type BTreeSecondaryIndex struct {
	baseTree *btree.BTree
}

func (B BTreeSecondaryIndex) Lookup(key int64) SearchBound {
	//TODO implement me
	panic("implement me")
}

func (B BTreeSecondaryIndex) Size() int64 {
	//TODO implement me
	panic("implement me")
}

func NewBtreeIndex() SecondaryIndex {
	return &BTreeSecondaryIndex{
		baseTree: &btree.BTree{},
	}
}
