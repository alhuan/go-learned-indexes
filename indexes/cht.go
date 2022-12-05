package indexes

import (
	"math"
	"math/bits"
	"unsafe"
)

type Info struct {
	first  uint64
	second uint64
}

type Range struct {
	first  uint64
	second uint64
}

type Tree struct {
	first  Info
	second []Range
}

type Elem struct {
	first  uint64
	second Range
}

type Optional struct {
	value uint64
}

type CompactHistTree struct {
	singleLayer bool
	minKey      uint64
	maxKey      uint64
	numKeys     uint64
	numBins     uint64
	logNumBins  uint64
	maxError    uint64
	shift       uint64

	keys    []uint64
	table   []uint64
	tree    []Tree
	prevKey uint64

	numRadixBits uint64
	numShiftBits uint64
}

func (cht *CompactHistTree) Lookup(key uint64) SearchBound {
	if !cht.singleLayer {
		begin := cht.lookup(key)
		var end uint64
		if begin+cht.maxError+1 > cht.numKeys {
			end = cht.numKeys
		} else {
			end = begin + cht.maxError + 1
		}
		return SearchBound{Start: begin, Stop: end}
	} else {
		var prefix = (key - cht.minKey) >> cht.shift
		begin := cht.table[prefix]
		end := cht.table[prefix+1]
		return SearchBound{Start: begin, Stop: end}

	}
}

func (cht *CompactHistTree) Size() int64 {
	return int64(unsafe.Sizeof(cht)) + int64(len(cht.table)*8)
}

func (cht *CompactHistTree) Name() string {
	return "CompactHistTree"
}

func NewCHT(data *[]KeyValue, minKey uint64, maxKey uint64, numBins uint64, maxError uint64) SecondaryIndex {
	cht := &CompactHistTree{}
	n := len(*data)
	cht.numKeys = uint64(n)
	cht.minKey = minKey
	cht.maxKey = maxKey
	cht.numBins = numBins
	cht.maxError = maxError
	cht.prevKey = minKey

	// Add each key
	for i := 0; i < n; i++ {
		cht.addKey((*data)[i].Key)
	}

	cht.finalize(numBins)
	return cht
}

// Private stuff
const (
	Leaf            = uint64(1) << 31
	Mask            = Leaf - 1
	Infinity uint64 = math.MaxInt64
)

func (cht *CompactHistTree) lookup(key uint64) uint64 {
	key -= cht.minKey
	width := cht.shift
	next := 0
	for true {
		var bin = key >> width
		next := cht.table[uint64(next<<cht.logNumBins)+bin]
		if next&Leaf == 1 {
			return next & Mask
		}
		key -= bin << width
		width -= cht.logNumBins
	}
	// shouldn't get here
	return -1
}

func (cht *CompactHistTree) addKey(key uint64) {
	cht.keys = append(cht.keys, key)
	cht.prevKey = key
}

func (cht *CompactHistTree) finalize(numBins uint64) {
	logNumBins := computeLog(uint64(numBins), false)
	lg := computeLog(cht.maxKey-cht.minKey, true)
	cht.shift = lg - logNumBins
	cht.buildOffline()
	cht.singleLayer = cht.flatten()
}

func computeLog(n uint64, round bool) uint64 {
	numLeadingZeros := uint64(bits.LeadingZeros64(n))
	if round {
		if n&(n-1) != 0 {
			return 63 - numLeadingZeros + 1
		} else {
			return 63 - numLeadingZeros
		}
	}
	return 63 - numLeadingZeros
}

func (cht *CompactHistTree) buildOffline() {
	initNode := func(nodeIndex uint64, curr Range) {
		var currBin *Optional = nil
		var width = cht.shift - cht.tree[nodeIndex].first.first*cht.logNumBins
		for index := curr.first; index != curr.second; index++ {
			bin := (cht.keys[index] - cht.minKey - cht.tree[nodeIndex].first.second) >> width
			if currBin != nil || bin != currBin.value {
				var iterValue uint64
				if currBin != nil {
					iterValue = currBin.value + 1
				} else {
					iterValue = 0
				}
				for iter := iterValue; iter != bin; iter++ {
					cht.tree[nodeIndex].second[iter] = Range{index, index}
				}
				cht.tree[nodeIndex].second[bin] = Range{index, index}
				nonNillOptional := Optional{value: bin}
				currBin = &nonNillOptional
			}
			cht.tree[nodeIndex].second[bin].second++
		}
	}

	ranges := make([]Range, cht.numBins)
	for i := range ranges {
		ranges[i] = Range{cht.numKeys, cht.numKeys}
	}
	cht.tree = append(cht.tree, Tree{first: Info{0, 0}, second: ranges})
	initNode(0, Range{0, uint64(cht.numKeys)})

	// Run the BFS
	var nodes []uint64
	nodes = append(nodes, 0)
	for len(nodes) != 0 {
		node := nodes[0]
		nodes = nodes[1:]
		var level = cht.tree[node].first.first
		var lower = cht.tree[node].first.second
		for bin := 0; uint64(bin) != cht.numBins; bin++ {
			if cht.tree[node].second[bin].second-cht.tree[node].second[bin].first > cht.maxError {
				size := cht.tree[node].second[bin].second - cht.tree[node].second[bin].first
				if size > (uint64(1) << (cht.shift - level*cht.logNumBins)) {
					cht.tree[node].second[bin].first |= Leaf
					continue
				}

				newNode := make([]Range, cht.numBins)
				for i := range newNode {
					newNode[i] = Range{cht.tree[node].second[bin].second, cht.tree[node].second[bin].second}
				}
				newLower := lower + uint64(bin)*(uint64(1)<<(cht.shift-level*cht.logNumBins))
				cht.tree = append(cht.tree, Tree{Info{level + 1, newLower}, newNode})
				initNode(uint64(len(cht.tree)-1), cht.tree[node].second[bin])
				cht.tree[node].second[bin] = Range{0, uint64(len(cht.tree) - 1)}
				nodes = append(nodes, uint64(len(cht.tree)-1))
			} else {
				// Leaf
				cht.tree[node].second[bin].first |= Leaf
			}
		}
	}

}

func (cht *CompactHistTree) flatten() bool {
	if len(cht.tree) == 1 {
		cht.transformIntoRadixTable()
		return true
	}

	limit := len(cht.tree)
	for index := 0; index != limit; index++ {
		for bin := uint64(0); bin != cht.numBins; bin++ {
			// Leaf node?
			if cht.tree[index].second[bin].first&Leaf == 1 {
				cht.table[uint64(index<<cht.logNumBins)+bin] = cht.tree[index].second[bin].first
			} else {
				cht.table[uint64(index<<cht.logNumBins)+bin] = cht.tree[index].second[bin].second
			}
		}
	}
	return false
}

func (cht *CompactHistTree) transformIntoRadixTable() {
	cht.numRadixBits = cht.logNumBins
	cht.numShiftBits = getNumShiftBits(cht.maxKey-cht.minKey, cht.numRadixBits)
	maxPrefix := (cht.maxKey - cht.minKey) >> cht.numShiftBits

	// equivalent of table.resize(max_prefix+2, 0)??
	for i := len(cht.table); uint64(i) < maxPrefix+2; i++ {
		cht.table = append(cht.table, 0)
	}
	var limit uint64
	if cht.numBins < uint64(len(cht.table)) {
		limit = cht.numBins
	} else {
		limit = uint64(len(cht.table))
	}
	for index := uint64(0); index != limit; index++ {
		cht.table[index] = cht.tree[0].second[index].first & Mask
	}
	cht.table[len(cht.table)-1] = cht.numKeys
	cht.shift = cht.numShiftBits

}
