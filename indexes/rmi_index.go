package indexes

// do a lookup by using layer1weight to derive the correct layer2 weight, then use the layer2 weight
// to perform a guess on positional information and bound it by max error bounds
// there's a bit of work to be done in figuring out which error bounds to use so whoever's doing this
// better read their papers good
type RMIIndex struct {
	layer1Weights []float32
	layer2Weights []float32
	maxError      uint64
}

func NewRMIIndex(keyValues *[]KeyValue) SecondaryIndex {
	return &RMIIndex{
		// TODO fill this in
	}
}

func (i *RMIIndex) Lookup(key uint64) SearchBound {
	//TODO implement me
	panic("implement me")
}

func (i *RMIIndex) Size() int64 {
	//TODO implement me
	panic("implement me")
}

func (i *RMIIndex) Name() string {
	return "RMI"
}
