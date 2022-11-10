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
func BinarySearch(data *[]indexes.KeyValue, lookupKey uint64, bound indexes.SearchBound) uint64 {
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
	if int(i) == len(*data) {
		return -1
	}
	return (*data)[i].Value
}
