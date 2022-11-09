package benchmark

import (
	"bufio"
	"errors"
	"go-learned-indexes/indexes"
	"log"
	"os"
	"strconv"
	"strings"
)

// TRINITY do these two functions

func LoadDataset(filename string) (*[]indexes.KeyValue, error) {
	// read a dataset from disk, read the values, and load it in
	f, err := os.Open(filename)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	data := []indexes.KeyValue{}
	for scanner.Scan() {
		text := scanner.Text()
		tokens := strings.Fields(text)
		if len(tokens) != 2 { // Assuming key value on each line
			return nil, errors.New("Line had more than 2 tokens")
		}
		intKey, _ := strconv.ParseInt(tokens[0], 0, 64)
		intValue, _ := strconv.ParseInt(tokens[1], 0, 64)
		item := indexes.KeyValue{Key: intKey, Value: intValue}
		data = append(data, item)

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	ptr := &data
	return ptr, nil
}

/**
* data will be sorted by KeyValue.Key
* lookupKey to be matched to KeyValue.Key
* bound representing SearchBound [start, stop)
* returns corresponding KeyValue.Value for lookupKey, -1 if not found or out of bounds
 */
func BinarySearch(data *[]indexes.KeyValue, lookupKey int64, bound indexes.SearchBound) int64 {
	// model the binary search off of the function body of sort.Search(), but we should probably use int64s instead of int32s
	// don't actually use this function, I'm just leaving it here so you can click into it for reference
	//sort.Search()

	mid := int64((bound.Start + bound.Stop) / 2)
	if mid == bound.Stop {
		return -1
	}
	if (*data)[mid].Key == lookupKey {
		return (*data)[mid].Value
	} else { // recursive binary search
		var newBound indexes.SearchBound
		if (*data)[mid].Key > lookupKey {
			newBound = indexes.SearchBound{Start: mid + 1, Stop: bound.Stop}
		} else {
			newBound = indexes.SearchBound{Start: bound.Start, Stop: mid}
		}
		return BinarySearch(data, lookupKey, newBound)
	}
}
