package benchmark

import "go-learned-indexes/indexes"

var (
	outputCSVDir = "./results"
	datasetDir = "./data"
)

func LoadDataset(filename string) *[]indexes.KeyValue {
	// TRINITY do this
	// read a dataset from disk, read the values, and load it in
	return nil
}

func RunAllIndexes() {
	// build all indexes and run them
}

func RunIndex(index indexes.SecondaryIndex, sizeScale int64) {
	bound := index.Lookup()
}
