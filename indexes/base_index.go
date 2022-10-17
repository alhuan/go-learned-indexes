package indexes

type LearnedIndex interface {
	Evaluate(key int64) int64
	Size() int64
}
