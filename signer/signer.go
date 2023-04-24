package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	inCh := make(chan interface{})
	outCh := make(chan interface{})

	var wg sync.WaitGroup

	for _, CurrJob := range jobs {
		wg.Add(1)
		go func(job job, in, out chan interface{}, w *sync.WaitGroup) {
			defer close(out)
			defer wg.Done()
			job(in, out)
		}(CurrJob, inCh, outCh, &wg)
		inCh = outCh
		outCh = make(chan interface{})
	}
	wg.Wait()
}

// type Item struct {
// 	Id  int
// 	Hsh string
// }

// type ById []Item

// func (a ById) Len() int           { return len(a) }
// func (a ById) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
// func (a ById) Less(i, j int) bool { return a[i].Id < a[j].Id }

func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	var m sync.Mutex

	for num := range in {
		fmt.Println("SingleHash got", num)
		intNum := num.(int)
		// if num != nil {
		// 	fmt.Println("OK: num is", num)
		// 	intNum = num.(int)
		// } else {
		// 	fmt.Println(">> num is", num)
		// }

		dataConn := make(chan string, 1)

		wg.Add(2)

		go func(in chan interface{}, connector chan string, data string, w *sync.WaitGroup, m *sync.Mutex) {
			defer w.Done()
			//defer close(connector)
			m.Lock()
			md5Data := DataSignerMd5(data)
			m.Unlock()
			connector <- DataSignerCrc32(md5Data)
		}(in, dataConn, strconv.Itoa(intNum), &wg, &m)

		go func(in chan interface{}, connector chan string, out chan interface{}, w *sync.WaitGroup, n int) {
			defer close(connector)
			defer w.Done()
			resultData := DataSignerCrc32(strconv.Itoa(n)) + "~" + (<-connector)
			fmt.Println(n, "SingleHash result", resultData)
			out <- resultData
		}(in, dataConn, out, &wg, intNum)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {

	for hash := range in {
		fmt.Println("MultiHash got", hash)
		Items := make([]string, 6)
		var wg sync.WaitGroup
		var m sync.Mutex
		var multiResult string

		wg.Add(6)
		for i := 0; i < 6; i++ {
			go func(data string, i int, results []string, wg *sync.WaitGroup, m *sync.Mutex) {
				defer wg.Done()
				hash := DataSignerCrc32(strconv.Itoa(i) + data)
				fmt.Println("Multihash", i, hash)
				m.Lock()
				results[i] = hash
				m.Unlock()
			}(hash.(string), i, Items, &wg, &m)
		}
		wg.Wait()
		// sort.Sort(ById(Items))

		// for _, item := range Items {
		// 	multiResult += item.Hsh
		// }
		multiResult = strings.Join(Items, "")
		fmt.Println("MultiHash sorted:", multiResult)
		out <- multiResult
	}
}

func CombineResults(in, out chan interface{}) {
	var allMultiHash []string
	for multiHash := range in {
		allMultiHash = append(allMultiHash, multiHash.(string))
	}
	sort.Strings(allMultiHash)
	joinedResult := strings.Join(allMultiHash, "_")
	fmt.Println("Combined:", joinedResult)
	out <- joinedResult
}
