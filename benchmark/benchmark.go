package benchmark

import (
	"fmt"
	"go-learned-indexes/indexes"
	"log"
	"os"
	"path"
	"runtime"
	"time"
)

var (
	outputCSVDir = "./results"
	datasetDir   = "./data"
	datasets     = []string{
		"fb_200M_uint64",
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
		loadedData, err := LoadDataset(path.Join(datasetDir, dataset))
		if err != nil {
			log.Fatal(err)
		}
		file := os.NewFile(0755, fmt.Sprintf("%s_results.csv", dataset))
		for _, creationFunc := range creationFuncs {
			// again, force a garbage collection to remove the previous index from memory
			// since it might still be there
			runtime.GC()
			buildStart := time.Now()
			index := creationFunc(loadedData)
			buildTime := time.Since(buildStart).Nanoseconds()
			var totalTime int64 = 0
			for _, lookupData := range *loadedData {
				// I think a GC pause here would actually cause this to run for hours so I'm not going to include it,
				// GC pauses  while the index runs are also a legitimate part of performance benchmarking anyway
				startTime := time.Now()
				bounds := index.Lookup(lookupData.Key)
				pos := BinarySearch(loadedData, lookupData.Key, bounds)
				if pos == -1 {
					log.Fatal(fmt.Sprintf("Bad lookup on index %s", index.Name()))
				}
				elapsed := time.Since(startTime).Nanoseconds()
				totalTime += elapsed
			}
			line := fmt.Sprintf("%s,%d,%f", index.Name(), buildTime, float64(totalTime)/float64(len(*loadedData)))
			if _, err := file.WriteString(line); err != nil {
				log.Fatal(err)
			}
			log.Print(line)
		}
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}
}
