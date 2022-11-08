package benchmark

import (
	"fmt"
	"go-learned-indexes/indexes"
	"os"
	"path"
	"runtime"
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
		// force a garbage collection to clean up the previous datasets so that
		// it doesn't continue to take up memory
		runtime.GC()
		loadedData := LoadDataset(path.Join(datasetDir, dataset))
		for _, creationFunc := range creationFuncs {
			// again, force a garbage collection to remove the previous index from memory
			// since it might still be there
			runtime.GC()
			index := creationFunc(loadedData)
			file := os.NewFile(0755, fmt.Sprintf("%s_%s.csv", dataset, index.Name()))
			
			file.Close()
		}
	}
}

func RunIndex(index indexes.SecondaryIndex, sizeScale int64, file *os.File, data *[]indexes.KeyValue) {

}
