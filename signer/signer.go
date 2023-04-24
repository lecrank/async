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
		// fmt.Println("START Job:", CurrJob)
		wg.Add(1)
		go func(job job, in, out chan interface{}) {
			defer close(out)
			defer wg.Done()
			// fmt.Println("Executing")
			job(in, out)
		}(CurrJob, inCh, outCh)
		inCh = outCh
		outCh = make(chan interface{})
	}
}

type Item struct {
	Id  int
	Hsh string
}

type ById []Item

func (a ById) Len() int           { return len(a) }
func (a ById) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ById) Less(i, j int) bool { return a[i].Id < a[j].Id }

func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	var m sync.Mutex

	for num := range in {
		fmt.Println("SingleHash got", num)

		dataConn := make(chan string, 1)

		wg.Add(2)

		go func(in chan interface{}, connector chan string, data string, w *sync.WaitGroup, m *sync.Mutex) {
			defer w.Done()
			//defer close(connector)
			m.Lock()
			md5Data := DataSignerMd5(data)
			m.Unlock()
			connector <- DataSignerCrc32(md5Data)
		}(in, dataConn, strconv.Itoa(num.(int)), &wg, &m)

		go func(in chan interface{}, connector chan string, out chan interface{}, w *sync.WaitGroup, n interface{}) {
			defer close(connector)
			defer w.Done()
			resultData := DataSignerCrc32(strconv.Itoa((<-in).(int))) + "~" + (<-connector)
			fmt.Println(n.(int), "SingleHash result", resultData)
			out <- resultData
		}(in, dataConn, out, &wg, num)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {

	for hash := range in {
		fmt.Println("MultiHash got", hash)
		var Items []Item
		var wg sync.WaitGroup
		var multiResult string

		wg.Add(6)
		for i := 0; i < 6; i++ {
			go func(data string, i int, results []Item, wg *sync.WaitGroup) {
				defer wg.Done()
				hash := DataSignerCrc32(strconv.Itoa(i) + data)
				fmt.Println("Multihash", i, hash)
				results = append(results, Item{i, hash})
			}(hash.(string), i, Items, &wg)
		}
		wg.Wait()
		sort.Sort(ById(Items))

		for _, item := range Items {
			multiResult += item.Hsh
		}
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
