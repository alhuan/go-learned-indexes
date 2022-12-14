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
		"osm_cellids_200M_uint64",
		"books_200M_uint64",
		"wiki_ts_200M_uint64",
		"normal_200M_uint64",
		"lognormal_200M_uint64",
		"uniform_sparse_200M_uint64",
		"uniform_dense_200M_uint64",
	}
	// we use creation funcs instead of storing the indices so that we can create them one at a time
	creationFuncs = []func(*[]indexes.KeyValue) indexes.SecondaryIndex{
		//// CHT
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewCHT(idxs, 32, 1024)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewCHT(idxs, 64, 512)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewCHT(idxs, 256, 256)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewCHT(idxs, 512, 128)
		//},
		//// binary search
		//indexes.NewBinarySearch,
		//// rbs
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixBinarySearch(idxs, 8)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixBinarySearch(idxs, 12)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixBinarySearch(idxs, 16)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixBinarySearch(idxs, 20)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixBinarySearch(idxs, 24)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixBinarySearch(idxs, 28)
		//},
		// btrees
		func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
			return indexes.NewBtreeIndex(idxs, 4)
		},
		func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
			return indexes.NewBtreeIndex(idxs, 16)
		},
		func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
			return indexes.NewBtreeIndex(idxs, 64)
		},
		func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
			return indexes.NewBtreeIndex(idxs, 256)
		},
		func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
			return indexes.NewBtreeIndex(idxs, 1024)
		},
		//// radixspline
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixSpline(idxs, 16, 220)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixSpline(idxs, 20, 160)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixSpline(idxs, 24, 70)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRadixSpline(idxs, 28, 80)
		//},
		// rmi
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRMIIndex(idxs, 128)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRMIIndex(idxs, 2048)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRMIIndex(idxs, 32768)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRMIIndex(idxs, 2097152)
		//},
		//func(idxs *[]indexes.KeyValue) indexes.SecondaryIndex {
		//	return indexes.NewRMIIndex(idxs, 33554432)
		//},
	}
	lookupsToGenerate = 10_000_000
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
		lookups := GenerateEqualityLookups(loadedData, lookupsToGenerate)
		file, err := os.OpenFile(fmt.Sprintf("%s_results.csv", dataset), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		for _, creationFunc := range creationFuncs {
			// again, force a garbage collection to remove the previous index from memory
			// since it might still be there
			//if datasetIdx > 2 && indexIdx < 4 {
			//	// CHT only works on the first 3 datasets...
			//	continue
			//}
			runtime.GC()
			buildStart := time.Now()
			index := creationFunc(loadedData)
			buildTime := time.Since(buildStart).Nanoseconds()
			var totalTime int64 = 0
			failed := false
			for _, lookupData := range lookups {
				// I think a GC pause here would actually cause this to run for hours so I'm not going to include it,
				// GC pauses  while the index runs are also a legitimate part of performance benchmarking anyway
				startTime := time.Now()
				bounds := index.Lookup(lookupData.Key)
				found := BinarySearch(loadedData, lookupData.Key, bounds)
				if !found {
					log.Print(fmt.Sprintf("Bad lookup on index %s on key %d, value %d and searchbound %+v", index.Name(), lookupData.Key, lookupData.Value, bounds))
					failed = true
				}
				elapsed := time.Since(startTime).Nanoseconds()
				totalTime += elapsed
			}
			// if we failed, don't record this time. it might be due to data duplicates or something
			if failed {
				continue
			}
			line := fmt.Sprintf("%s, %d, %d, %f\n", index.Name(), buildTime, index.Size(), float64(totalTime)/float64(len(lookups)))
			log.Print(line)
			if _, err := file.WriteString(line); err != nil {
				log.Print(err)
			}
		}
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}
}
