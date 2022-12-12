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
	layer2_size int64
	l1          Model
	l2          []Model

	errors []int64
}

func NewRMIIndex[Layer1 Model, Layer2 Model](keyValues *[]KeyValue, layer2_size int64) SecondaryIndex {
	rmi := &RMIIndex{}

	rmi.layer2_size = layer2_size
	rmi.n_keys = int64(len(*keyValues))

	// Train layer1 with compression.
	var l1 Layer1
	l1.Train(*keyValues, 0, float64(rmi.layer2_size)/float64(rmi.n_keys))
	rmi.l1 = l1

	// Train layer2 models.
	var segment_start int64 = 0
	var segment_id int64 = 0
	for pos, val := range *keyValues {
		var i int64 = int64(pos)
		var pred_segment_id int64 = rmi.getSegmentId(val.Key)
		if pred_segment_id > segment_id {
			var l2 Layer2
			l2.Train((*keyValues)[segment_start:i], segment_start, 1)
			rmi.l2 = append(rmi.l2, l2)
			for j := segment_id + 1; j < pred_segment_id; j++ {
				var newl2 Layer2
				newl2.Train((*keyValues)[i-1:i], i-1, 1)
				rmi.l2 = append(rmi.l2, newl2)
			}
			segment_id = pred_segment_id
			segment_start = i
		}
	}
	// Train remaining models.
	var l2 Layer2
	l2.Train((*keyValues)[segment_start:], segment_start, 1)
	rmi.l2 = append(rmi.l2, l2)
	for j := segment_id + 1; j < rmi.layer2_size; j++ {
		// Train remaining models on last key.
		var newl2 Layer2
		newl2.Train((*keyValues)[rmi.n_keys-1:], rmi.n_keys-1, 1)
		rmi.l2 = append(rmi.l2, newl2)
	}

	rmi.errors = make([]int64, layer2_size)

	// Compute local abosolute error bounds.
	for pos, val := range *keyValues {
		var i, segment_id int64
		i = int64(pos)
		segment_id = rmi.getSegmentId(val.Key)
		pred := clamp(int64(rmi.l2[segment_id].Predict(val.Key)), 0, rmi.n_keys-1)
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
	prediction := int64(rmi.l2[segment_id].Predict(key))
	var lo uint64 = uint64(clamp(prediction-rmi.errors[segment_id], 0, rmi.n_keys))
	var hi uint64 = uint64(clamp(prediction+rmi.errors[segment_id] + 1, 0, rmi.n_keys))
	// FIXME: add bounds
	return SearchBound{lo, hi}
}

func (rmi *RMIIndex) Size() int64 {
	return int64(unsafe.Sizeof(*rmi)) + rmi.l1.Size() + rmi.l2[0].Size()*rmi.layer2_size
}

func (rmi *RMIIndex) Name() string {
	return "RMI"
}
