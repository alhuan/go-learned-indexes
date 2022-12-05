package indexes

import "math"
import "unsafe"

// do a lookup by using layer1weight to derive the correct layer2 weight, then use the layer2 weight
// to perform a guess on positional information and bound it by max error bounds
// there's a bit of work to be done in figuring out which error bounds to use so whoever's doing this
// better read their papers good
type RMIIndex struct {
	maxError		uint64

	n_keys			int64
	layer2_size 	int64
	l1				Model
	l2				[]Model
}

func NewRMIIndex(keyValues *[]KeyValue, maxError uint64) SecondaryIndex {
	rmi := &RMIIndex{}

	rmi.layer2_size = 2
	rmi.n_keys = int64(len(*keyValues))
	rmi.maxError = maxError

	// Train layer1 with compression.
	rmi.l1 = NewLinearRegression(*keyValues, 0, float64(rmi.layer2_size) / float64(rmi.n_keys))

	// Train layer2 models.
	var segment_id, segment_start int64
	segment_start = 0
	segment_id = 0
	for pos, val := range *keyValues {
		var i, pred_segment_id int64
		i = int64(pos)
		pred_segment_id = rmi.getSegmentId(val.Key)
		if pred_segment_id > segment_id {
			rmi.l2 = append(rmi.l2, NewLinearRegression((*keyValues)[segment_start: i], segment_start, 1))
			for j := segment_id; j < pred_segment_id; j ++ {
				rmi.l2 = append(rmi.l2, NewLinearRegression((*keyValues)[i - 1: i], i - 1, 1))
			}
			segment_id = pred_segment_id
			segment_start = i
		}
	}
	// Train remaining models.
	rmi.l2[segment_id] = NewLinearRegression((*keyValues)[segment_start:], segment_start, 1)
	for j := segment_id + 1; j < rmi.layer2_size; j ++ {
		// Train remaining models on last key.
		rmi.l2 = append(rmi.l2, NewLinearRegression((*keyValues)[rmi.n_keys - 1:], rmi.n_keys - 1, 1))
	}

	return rmi
}

// Returns the segment id for the 2nd layer.
func (rmi *RMIIndex) getSegmentId(key uint64) int64 {
	prediction := rmi.l1.Predict(key)
	return int64(math.Max(math.Min(prediction, float64(rmi.layer2_size - 1)), 0))
}

func (rmi *RMIIndex) Lookup(key uint64) SearchBound {
	prediction := rmi.l2[rmi.getSegmentId(key)].Predict(key)
	var index uint64 
	index = uint64(math.Max(math.Min(prediction, float64(rmi.n_keys - 1)), 0))
	return SearchBound{index - rmi.maxError, index + rmi.maxError}
}

func (rmi *RMIIndex) Size() int64 {
	return int64(unsafe.Sizeof(*rmi)) + rmi.l1.Size() + rmi.l2[0].Size() * rmi.layer2_size
}

func (rmi *RMIIndex) Name() string {
	return "RMI"
}
