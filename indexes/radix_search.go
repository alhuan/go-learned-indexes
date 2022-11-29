package indexes

import (
	"math/bits"
	"unsafe"
)

type Coord struct {
	x 	uint64
	y 	float64
}

type RadixSearch struct {
	minKey 				uint64
	maxKey 				uint64
	numKeys 			uint64
	numRadixBits 		uint64
	numShiftBits 		uint64
	maxError 			uint64

	radixTable 			[]uint64
	splinePoints 		[]Coord


	// maybe don't need this if all the building can be done in one function?
	curNumKeys 			uint64
	curNumDistinctKeys	uint64
	prevKey 			uint64
	prevPosition 		uint64
	prevPrefix 			uint64

	upperLimit 			Coord
	lowerLimit 			Coord
	prevPoint 			Coord
}

func (r *RadixSearch) Lookup(key uint64) SearchBound {
	estimate := getEstimatedPosition(key)
	begin := (estimate < r.maxError) ? 0 : (estimate - r.maxError) // AGAIN DOES GO HAVE TERNARY
	end := (estimate + r.maxError + 2 > r.numKeys) ? r.numKeys : (estimate + r.maxError + 2)
    return SearchBound{begin, end}
}

func (r *RadixSearch) getEstimatedPosition(key uint64) int {
    if (key <= r.minKey) {
		return 0
	}
    if (key >= r.maxKey) {
		return r.numKeys - 1
	}

	index := getSplineSegment(key)

    down := r.splinePoints[index - 1]
    up := r.splinePoints[index]

    x_diff := up.x - down.x
    y_diff := up.y - down.y
    slope := y_diff / x_diff

    // Interpolate.
    key_diff := key - down.x
    return std::fma(key_diff, slope, down.y) //TODO: WHAT IS FMA IN GO ?? and in c++ lol 
}

func getSplineSegment(key uint64) int {
	prefix := = (key - r.minKey) >> r.numShiftBits
	begin := r.radixTable[prefix]
	end := r.radixTable[prefix + 1]

    if (end - begin < 32) {
    	current := begin
		while (r.splinePoints[current].x < key) {
			current ++
		}
		return current
    }


	//TODO::: ER I HAVE NO IDEA HOW TO CHANGE THIS PART LOL
	const auto lb = std::lower_bound(
        spline_points_.begin() + begin, spline_points_.begin() + end, key,
        [](const Coord<KeyType>& coord, const KeyType key) {
          return coord.x < key;
        });
    return std::distance(spline_points_.begin(), lb);
  }

func (r *RadixSearch) Size() int64 {
	return int64(unsafe.Sizeof(*r))

	//TODO :::: THIS NEEDS TO CHANGE BUT IDK WHAT SIZE OF IS LOL
	return unsafe.Sizeof(*r) + r.radixTable.size() * sizeof(uint32_t) + r.splinePoints.size() * sizeof(Coord<uint32>)
}

func (r *RadixSearch) Name() string {
	return "RadixSearch"
}

func NewRadixSearch(data *[]KeyValue, numRadixBits uint64, maxError uint64) SecondaryIndex {
	n := len(*data)

	rs := &RadixSearch{}

	rs.minKey = (*data)[0].Key
	rs.maxKey = (*data)[n-1].Key
    max_prefix := (rs.maxKey - rs.minKey) >> numRadixBits
	rdx.radixTable = make([]uint64, max_prefix + 2) //how to fill this with all 0's?

	for i := 0; i < n; i++ {
		curKey := (*data)[i].Key

		// L O L IS POSITION I ???????

		//AddKey {
		//PossiblyAddKeyToSpline {
		if (curNumKeys == 0) {
			rs.addKeyToSpline(curKey, i)
			curNumDistinctKeys++;

			//RememberPreviousCDFPoint 
			prevPoint = Coord{curKey, i};
			continue
		}
	
		if (curKey == prevKey) {
			continue
		}
	
		curNumDistinctKeys++

		if (curNumDistinctKeys == 2) {
			upperLimit = Coord{curKey, i}
			lowerLimit = Coord{curKey, (position < rs.maxError) ? 0 : position - rs.maxError}
			prevPoint = Coord{curKey, i}
			continue
		}
	
		last := splinePoints[len(splinePoints) - 1] // IDK DOES THIS DO :: const Coord<KeyType>& last = spline_points_.back();

		upper_y := i + maxError
		lower_y := (i < maxError) ? 0 : i - maxError //DOES go have ternary?????

		upper_limit_x_diff := upperLimit.x - last.x
		lower_limit_x_diff := lowerLimit.x - last.x
		x_diff := curKey - last.x
		upper_limit_y_diff := upperLimit.y - last.y
		lower_limit_Y_diff := lowerLimit.y - last.y
		y_diff := i - last.y



		if ((ComputeOrientation(upper_limit_x_diff, upper_limit_y_diff, x_diff, y_diff) != Orientation::CW) || //fix for golang enums
			(ComputeOrientation(lower_limit_x_diff, lower_limit_y_diff, x_diff,y_diff) != Orientation::CCW)) {
			rs.addKeyToSpline(prevPoint.x, prevPoint.y) //idk if the types will match up for y
		
			upperLimit = Coord{curKey, upper_y}
			lowerLimit = Coord{curKey, lower_y}
		} else {
			upper_y_diff := upper_y - last.y

			if (ComputeOrientation(upper_limit_x_diff, upper_limit_y_diff, x_diff,upper_y_diff) == Orientation::CW) {
				upperLimit = Coord{curKey, upper_y}
			}
	
			lower_y_diff := lower_y - last.y
			if (ComputeOrientation(lower_limit_x_diff, lower_limit_y_diff, x_diff,lower_y_diff) == Orientation::CCW) {
				lowerLimit = Coord{curKey, lower_y}
			}
		}
	
		//RememberPreviousCDFPoint(key, position);
		prevPoint = Coord{curKey, i}

		// } end of PossiblyAddKeyToSpline
		
		curNumKeys++
		prevKey = key
		prevPosition = position

		// } end of AddKey
	}


	//RadixSpline<KeyType> Finalize() {
    if (curNumKeys > 0 && rs.splinePoints.back().x != prevKey) { //TODO: CHANGE the .back to get the last element in golang
		addKeyToSpline(prevKey, prevPosition)
	}

    // FinalizeRadixTable(); {
	prevPrefix++
    for (; prevPrefix < len(rs.radixTable); prevPrefix++) {
      rs.radixTable[prevPrefix] = len(rs.splinePoints)
	}
	
	// } end of FinalizeRadixTable()


	// DOES THIS MATTER ??? should i make deep copies of spline points and radix table ???
    // return RadixSpline<KeyType>(
    //     min_key_, max_key_, curr_num_keys_, num_radix_bits_, num_shift_bits_,
    //     max_error_, std::move(radix_table_), std::move(spline_points_));


	// end of Finalize } 

	return rs
}


func (r *RadixSearch) addKeyToSpline(curKey uint64, i int) { //IS THIS HOW TO MAKE VOID FUNCTION?
	r.splinePoints = append(r.splinePoints, Coord{curKey, i}) //is this super inefficient??

	// PossiblyAddKeyToRadixTable(key) {
	curPrefix := (curKey - rs.minKey) >> rs.numShiftBits
	if (curPrefix != rs.prevPrefix) {
		curIndex := len(rs.splinePoints) - 1
		for pref := rs.prevPrefix + 1; pref <= curPrefix; pref++ {
			rs.radixTable[pref] = curIndex
		}
		rs.prevPrefix = curPrefix
	}
	// }end of PossiblyAddKeyToRadixTable
}




//TODO: CHANGE THIS WHOLE THING TO GO LOL DOES GOLANG HAVE ENUMS HOPEFULLy
enum Orientation { Collinear, CW, CCW };
static constexpr double precision = std::numeric_limits<double>::epsilon();

static Orientation ComputeOrientation(const double dx1, const double dy1, const double dx2, const double dy2) {
	const double expr = std::fma(dy1, dx2, -std::fma(dy2, dx1, 0));
	if (expr > precision)
		return Orientation::CW;
	else if (expr < -precision)
		return Orientation::CCW;
	return Orientation::Collinear;
};
