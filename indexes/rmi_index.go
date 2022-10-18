package indexes

type RMIIndex struct {
	layer1Weights []float32
	layer2Weights []float32
}

func NewRMIIndex() SecondaryIndex {
	return &RMIIndex{
		// TODO fill this in
	}
}

func (i *RMIIndex) Lookup(key int64) SearchBound {
	//TODO implement me
	panic("implement me")
}

func (i *RMIIndex) Size() int64 {
	//TODO implement me
	panic("implement me")
}
