package indexes

type RMIIndex struct {
	layer1Weights []float32
	layer2Weights []float32
}

func NewRMIIndex() LearnedIndex {
	return &RMIIndex{
		// TODO fill this in
	}
}

func (i *RMIIndex) Evaluate(key int64) int64 {
	//TODO implement me
	panic("implement me")
}

func (i *RMIIndex) Size() int64 {
	//TODO implement me
	panic("implement me")
}
