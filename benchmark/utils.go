package benchmark

import (
	"encoding/binary"
	"fmt"
	"go-learned-indexes/indexes"
	"log"
	"os"
)

func LoadDataset(filename string) (*[]indexes.KeyValue, error) {
	// read a dataset from disk, read the values, and load it in
	file, err := os.Open(filename)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	var size uint64
	if err = binary.Read(file, binary.LittleEndian, &size); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Reading dataset of size %d", size)
	// we need to make a slice of the correct size, then
	data := make([]uint64, size)
	if err = binary.Read(file, binary.LittleEndian, &data); err != nil {
		log.Fatal(err)
	}

	keyValues := make([]indexes.KeyValue, size)
	for pos, key := range data {
		keyValues[pos] = indexes.KeyValue{Key: key, Value: uint64(pos)}
	}
	return &keyValues, nil
}

/**
* data will be sorted by KeyValue.Key
* lookupKey to be matched to KeyValue.Key
* bound representing SearchBound [start, stop)
* returns corresponding KeyValue.Value for lookupKey, -1 if not found or out of bounds
 */
func BinarySearch(data *[]indexes.KeyValue, lookupKey uint64, bound indexes.SearchBound) bool {
	// model the binary search off of the function body of sort.Search(), but we should probably use int64s instead of int32s
	// don't actually use this function, I'm just leaving it here so you can click into it for reference
	i, j := bound.Start, bound.Stop
	for i < j {
		h := (i + j) >> 1
		// i ≤ h < j
		if (*data)[h].Key < lookupKey {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	return (*data)[i].Key == lookupKey
}
