package indexes

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
)

func TestIndex(t *testing.T) {
	// FILL IN INDEX HERE
	var index SecondaryIndex

	intSet := make(map[uint64]struct{})
	for len(intSet) < 10_000 {
		randomInt := rand.Uint64()
		if _, ok := intSet[randomInt]; ok {
			continue
		}
		intSet[randomInt] = struct{}{}
	}

	var intList []uint64
	for k, _ := range intSet {
		intList = append(intList, k)
	}
	sort.Slice(intList, func(i, j int) bool { return intList[i] < intList[j] })

	var keysList []KeyValue
	for i, uint := range intList {
		keysList = append(keysList, KeyValue{Key: uint, Value: uint64(i)})
	}

	// PUT CONSTRUCTION FUNCTION HERE I GUESS?
	index = NewBtreeIndex(&keysList, 4)

	for _, keyValue := range keysList {
		searchRange := index.Lookup(keyValue.Key)
		found := false
		// linear search because im lazy
		for i := searchRange.Start; i < searchRange.Stop; i++ {
			if keysList[i].Key == keyValue.Key {
				found = true
			}
		}
		if !found {
			toPrint := "Not found " + strconv.Itoa(int(keyValue.Key)) + ", range was " + fmt.Sprintf("%#v", searchRange)
			t.Fatal(toPrint)
		}
	}
}
