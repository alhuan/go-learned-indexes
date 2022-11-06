package benchmark

import (
	"go-learned-indexes/indexes"
	"path"
)

var (
	outputCSVDir = "./results"
	datasetDir   = "./data"
	datasets     = []string{
		// TODO fill in all datasets here after generating them
	}
	// we use creation funcs instead of storing the indices so that we can create them one at a time
	creationFuncs = []func(*[]indexes.KeyValue) indexes.SecondaryIndex{
		indexes.NewBtreeIndex,
		indexes.NewRMIIndex,
	}
)

func RunAllIndexes() {
	// build all indexes and run them

	for _, dataset := range datasets {
		loadedData := LoadDataset(path.Join(datasetDir, dataset))
		for _, creationFunc := range creationFuncs {

		}
	}
}

func RunIndex(index indexes.SecondaryIndex, sizeScale int64) {

}
