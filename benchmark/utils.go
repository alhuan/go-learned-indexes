package benchmark

import (
	"bufio"
	"errors"
	"go-learned-indexes/indexes"
	"log"
	"os"
	"sort"
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
func BinarySearch(data *[]indexes.KeyValue, lookupKey int64, bound indexes.SearchBound) int64 {
	// model the binary search off of the function body of sort.Search(), but we should probably use int64s instead of int32s
	// don't actually use this function, I'm just leaving it here so you can click into it for reference
	sort.Search()

	return -1
}
