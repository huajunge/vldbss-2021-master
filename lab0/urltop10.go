package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

// ExampleURLCountMap is the map function in the first round
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	localURLCount := make(map[string]int)
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		if _, err := localURLCount[l]; !err {
			localURLCount[l] = 0
		}
		localURLCount[l]++
	}
	for url, count := range localURLCount {
		kvs = append(kvs, KeyValue{url, strconv.Itoa(count)})
	}
	return kvs
}

// ExampleURLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	count := 0
	for _, value := range values {
		v, err := strconv.Atoi(value)
		if err != nil {
			// handle error
			fmt.Println(err)
		}
		count += v
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(count))
}

// ExampleURLTop10Map is the map function in the second round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, 10)
	localCount := make(map[string]int)
	for _, l := range lines {
		v := strings.TrimSpace(l)
		if len(v) == 0 {
			continue
		}
		kv := strings.Split(l, " ")
		count, err := strconv.Atoi(kv[1])
		if err != nil {
			panic(err)
		}
		localCount[kv[0]] = count
		//kvs = append(kvs, KeyValue{"", l})
	}

	us, cs := TopN(localCount, 10)
	for i := range us {
		kvs = append(kvs, KeyValue{Key: "", Value: fmt.Sprintf("%s %d", us[i], cs[i])})
	}
	return kvs
}

// ExampleURLTop10Reduce is the reduce function in the second round
func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
