package indexes

import (
	"github.com/DmitriyVTitov/size"
	"math/bits"
)

type RadixBinarySearch struct {
	n            uint64
	min          uint64
	max          uint64
	shiftBits    uint64
	numRadixBits uint32
	radixHints   []uint32
}

func (r *RadixBinarySearch) Lookup(key uint64) SearchBound {
	p := (key - r.min) >> r.shiftBits
	if p > uint64(len(r.radixHints)-2) {
		p = uint64(len(r.radixHints) - 2)
	}

	begin := r.radixHints[p]
	end := r.radixHints[p+1]
	if begin != 0 {
		begin--
	}
	if uint64(end) != r.n {
		end++
	}
	return SearchBound{uint64(begin), uint64(end)}
}

func (r *RadixBinarySearch) Size() int64 {
	return int64(size.Of(r))
}

func (r *RadixBinarySearch) Name() string {
	return "RadixBinarySearch"
}

func NewRadixBinarySearch(data *[]KeyValue, numRadixBits uint32) SecondaryIndex {
	n := len(*data)

	rbx := &RadixBinarySearch{}
	rbx.numRadixBits = numRadixBits
	rbx.radixHints = make([]uint32, (1<<numRadixBits)+1)
	rbx.min = (*data)[0].Key
	rbx.max = (*data)[n-1].Key
	rbx.n = uint64(n)
	rbx.shiftBits = rbx.shiftBitsVal(rbx.max - rbx.min)

	rbx.radixHints[0] = 0
	var prevPrefix uint64 = 0

	for i := 0; i < n; i++ {
		currPrefix := ((*data)[i].Key - rbx.min) >> rbx.shiftBits
		if currPrefix != prevPrefix {
			for j := prevPrefix + 1; j <= currPrefix; j++ {
				rbx.radixHints[j] = uint32(i)
			}
			prevPrefix = currPrefix
		}
	}
	for ; prevPrefix < (1 << numRadixBits); prevPrefix++ {
		rbx.radixHints[prevPrefix+1] = uint32(n)
	}

	return rbx
}
func (r *RadixBinarySearch) shiftBitsVal(val uint64) uint64 {
	clz := bits.LeadingZeros64(val)
	if 64-clz < int(r.numRadixBits) {
		return 0
	}
	return 64 - uint64(r.numRadixBits) - uint64(clz)
}
