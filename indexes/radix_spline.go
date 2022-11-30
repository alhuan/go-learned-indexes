package indexes

import (
	"math"
	"unsafe"
)

type Coord struct {
	x uint64
	y float64
}

type RadixSpline struct {
	minKey       uint64
	maxKey       uint64
	numKeys      uint64
	numRadixBits uint64
	numShiftBits uint64
	maxError     uint64

	radixTable   []uint64
	splinePoints []Coord

	prevPrefix uint64
}

func (r *RadixSpline) Lookup(key uint64) SearchBound {
	estimate := r.getEstimatedPosition(key)
	var begin uint64
	var end uint64
	if estimate < r.maxError {
		begin = 0
	} else {
		begin = estimate - r.maxError
	}
	if estimate+r.maxError+2 > r.numKeys {
		end = r.numKeys
	} else {
		end = estimate + r.maxError + 2
	}
	return SearchBound{begin, end}
}

func (r *RadixSpline) getEstimatedPosition(key uint64) uint64 {
	if key <= r.minKey {
		return 0
	}
	if key >= r.maxKey {
		return r.numKeys - 1
	}

	index := r.getSplineSegment(key)

	down := r.splinePoints[index-1]
	up := r.splinePoints[index]

	xDiff := float64(up.x - down.x)
	yDiff := up.y - down.y
	slope := yDiff / xDiff
	keyDiff := float64(key - down.x)
	return uint64(math.FMA(keyDiff, slope, down.y)) // x * y + z, computed with only one rounding.
}

func (r *RadixSpline) getSplineSegment(key uint64) uint64 {
	prefix := (key - r.minKey) >> r.numShiftBits
	begin := r.radixTable[prefix]
	end := r.radixTable[prefix+1]

	if end-begin < 32 {
		current := begin
		for r.splinePoints[current].x < key {
			current++
		}
		return current
	}

	count := end - begin + 1 //is it inclusive?
	var it uint64
	for count > 0 {
		it = begin
		step := count / 2
		it += step
		if r.splinePoints[it].x < key {
			it++
			begin = it
			count -= step + 1
		} else {
			count = step
		}
	}
	return begin
}

func (r *RadixSpline) Size() int64 {
	return int64(unsafe.Sizeof(*r)) + int64(len(r.radixTable)*8+len(r.splinePoints)*16)
}

func (r *RadixSpline) Name() string {
	return "RadixSearch"
}

func NewRadixSpline(data *[]KeyValue, numRadixBits uint64, maxError uint64) SecondaryIndex {
	n := len(*data)
	rs := &RadixSpline{}
	var curNumKeys uint64
	var curNumDistinctKeys uint64
	var prevKey uint64
	var prevPosition uint64
	var upperLimit Coord
	var lowerLimit Coord
	var prevPoint Coord

	rs.minKey = (*data)[0].Key
	rs.maxKey = (*data)[n-1].Key
	maxPrefix := (rs.maxKey - rs.minKey) >> numRadixBits
	rs.radixTable = make([]uint64, maxPrefix+2) //how to fill this with all 0's?
	for i := 0; uint64(i) < maxPrefix+2; i++ {  // ^ sus
		rs.radixTable[i] = 0
	}

	for i := 0; i < n; i++ {
		curKey := (*data)[i].Key
		pos := float64(i)
		maxErrorF := float64(maxError)

		//AddKey {
		//PossiblyAddKeyToSpline {
		if curNumKeys == 0 {
			rs.addKeyToSpline(curKey, pos)
			curNumDistinctKeys++

			//RememberPreviousCDFPoint
			prevPoint = Coord{curKey, pos}
			continue
		}

		if curKey == prevKey {
			continue
		}
		curNumDistinctKeys++

		if curNumDistinctKeys == 2 {
			upperLimit = Coord{curKey, pos}
			if pos < maxErrorF {
				lowerLimit = Coord{curKey, 0}
			} else {
				lowerLimit = Coord{curKey, pos - maxErrorF}
			}
			prevPoint = Coord{curKey, pos}
			continue
		}

		last := rs.splinePoints[len(rs.splinePoints)-1]

		upperY := pos + maxErrorF
		var lowerY float64
		if pos < maxErrorF {
			lowerY = 0
		} else {
			lowerY = pos - maxErrorF
		}

		upperLimitXDiff := float64(upperLimit.x - last.x)
		lowerLimitXDiff := float64(lowerLimit.x - last.x)
		xDiff := float64(curKey - last.x)
		upperLimitYDiff := upperLimit.y - last.y
		lowerLimitYDiff := lowerLimit.y - last.y
		yDiff := pos - last.y

		if computeOrientation(upperLimitXDiff, upperLimitYDiff, xDiff, yDiff) != CW || computeOrientation(lowerLimitXDiff, lowerLimitYDiff, xDiff, yDiff) != CCW {
			rs.addKeyToSpline(prevPoint.x, prevPoint.y)

			upperLimit = Coord{curKey, upperY}
			lowerLimit = Coord{curKey, lowerY}
		} else {
			upperYDiff := upperY - last.y
			if computeOrientation(upperLimitXDiff, upperLimitYDiff, xDiff, upperYDiff) == CW {
				upperLimit = Coord{curKey, upperY}
			}
			lowerYDiff := lowerY - last.y
			if computeOrientation(lowerLimitXDiff, lowerLimitYDiff, xDiff, lowerYDiff) == CCW {
				lowerLimit = Coord{curKey, lowerY}
			}
		}

		//RememberPreviousCDFPoint(key, position);
		prevPoint = Coord{curKey, pos}
		// } end of PossiblyAddKeyToSpline

		curNumKeys++
		prevKey = curKey
		prevPosition = uint64(i)
		// } end of AddKey
	}

	//RadixSpline<KeyType> Finalize() {
	if curNumKeys > 0 && rs.splinePoints[len(rs.splinePoints)-1].x != prevKey {
		rs.addKeyToSpline(prevKey, float64(prevPosition))
	}

	// FinalizeRadixTable(); {
	rs.prevPrefix++
	for ; rs.prevPrefix < uint64(len(rs.radixTable)); rs.prevPrefix++ {
		rs.radixTable[rs.prevPrefix] = uint64(len(rs.splinePoints))
	}

	// } end of FinalizeRadixTable()

	// i don't think i need to make a copy because go doesn't pass by reference?

	// return RadixSpline<KeyType>(
	//     min_key_, max_key_, curr_num_keys_, num_radix_bits_, num_shift_bits_,
	//     max_error_, std::move(radix_table_), std::move(spline_points_));

	// end of Finalize }

	return rs
}

func (r *RadixSpline) addKeyToSpline(curKey uint64, i float64) {
	r.splinePoints = append(r.splinePoints, Coord{curKey, i}) //is this super inefficient??
	// PossiblyAddKeyToRadixTable(key) {
	curPrefix := (curKey - r.minKey) >> r.numShiftBits
	if curPrefix != r.prevPrefix {
		curIndex := len(r.splinePoints) - 1
		for pref := r.prevPrefix + 1; pref <= curPrefix; pref++ {
			r.radixTable[pref] = uint64(curIndex)
		}
		r.prevPrefix = curPrefix
	}
	// }end of PossiblyAddKeyToRadixTable
}

type Orientation int

const (
	Collinear Orientation = 0
	CW                    = 1
	CCW                   = 2
)

func computeOrientation(dx1 float64, dy1 float64, dx2 float64, dy2 float64) Orientation {
	expr := math.FMA(dy1, dx2, -math.FMA(dy2, dx1, 0))
	precision := math.Nextafter(1, 2) - 1
	if expr > precision {
		return CW
	} else if expr < -precision {
		return CCW
	} else {
		return Collinear
	}
}
