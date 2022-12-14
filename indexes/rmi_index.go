package indexes

import (
	"math"
)
import "unsafe"

func clamp(val int64, lo int64, hi int64) int64 {
	if val < lo {
		return lo
	}
	if hi < val {
		return hi
	}
	return val
}

// do a lookup by using layer1weight to derive the correct layer2 weight, then use the layer2 weight
// to perform a guess on positional information and bound it by max error bounds
// there's a bit of work to be done in figuring out which error bounds to use so whoever's doing this
// better read their papers good
type RMIIndex struct {
	n_keys      int64
	layer2_size int
	l1          LinearRegression
	l2          []LinearRegression

	errors []int64
}

func NewRMIIndex(keyValues *[]KeyValue, layer2_size int) SecondaryIndex {
	n := len(*keyValues)
	rmi := &RMIIndex{}

	rmi.layer2_size = layer2_size
	rmi.n_keys = int64(n)

	// Train layer1 with compression.
	var l1 LinearRegression
	l1.Train(keyValues, 0, n, float64(rmi.layer2_size)/float64(rmi.n_keys))
	rmi.l1 = l1

	// Train layer2 models.
	var segment_start int = 0
	var segment_id int = 0
	for pos := 0; pos < n; pos ++ {
		var pred_segment_id int = int(rmi.getSegmentId((*keyValues)[pos].Key))
		if pred_segment_id > segment_id {
			var l2 LinearRegression
			l2.Train(keyValues, segment_start, pos, 1)
			rmi.l2 = append(rmi.l2, l2)
			for j := segment_id + 1; j < pred_segment_id; j++ {
				var newl2 LinearRegression
				l2.Train(keyValues, pos - 1, pos, 1)
				rmi.l2 = append(rmi.l2, newl2)
			}
			segment_id = pred_segment_id
			segment_start = pos
		}
	}
	// Train remaining models.
	var l2 LinearRegression
	l2.Train(keyValues, segment_start, n, 1)
	rmi.l2 = append(rmi.l2, l2)
	for j := segment_id + 1; j < rmi.layer2_size; j++ {
		// Train remaining models on last key.
		var newl2 LinearRegression
		l2.Train(keyValues, n - 1, n, 1)
		rmi.l2 = append(rmi.l2, newl2)
	}

	rmi.errors = make([]int64, layer2_size)

	// Compute local absolute error bounds.
	for pos := 0; pos < n; pos ++ {
		var i, segment_id int64
		key := (*keyValues)[pos].Key
		i = int64(pos)
		segment_id = rmi.getSegmentId(key)
		pred := clamp(int64(rmi.l2[segment_id].Predict(key)), 0, rmi.n_keys-1)
		if pred > i {
			rmi.errors[segment_id] = int64(math.Max(float64(rmi.errors[segment_id]), float64(pred-i)))
		} else {
			rmi.errors[segment_id] = int64(math.Max(float64(rmi.errors[segment_id]), float64(i-pred)))
		}
	}

	return rmi
}

// Returns the segment id for the 2nd layer.
func (rmi *RMIIndex) getSegmentId(key uint64) int64 {
	prediction := rmi.l1.Predict(key)
	return int64(math.Max(math.Min(prediction, float64(rmi.layer2_size-1)), 0))
}

func (rmi *RMIIndex) Lookup(key uint64) SearchBound {
	segment_id := rmi.getSegmentId(key)
	prediction := clamp(int64(rmi.l2[segment_id].Predict(key)), 0, rmi.n_keys-1)
	var lo uint64 = uint64(clamp(prediction-rmi.errors[segment_id], 0, rmi.n_keys - 1))
	var hi uint64 = uint64(clamp(prediction+rmi.errors[segment_id] + 1, 0, rmi.n_keys))
	// FIXME: add bounds
	return SearchBound{lo, hi}
}

func (rmi *RMIIndex) Size() int64 {
	return int64(unsafe.Sizeof(*rmi)) + rmi.l1.Size() + rmi.l2[0].Size() * int64(rmi.layer2_size)
}

func (rmi *RMIIndex) Name() string {
	return "RMI"
}
