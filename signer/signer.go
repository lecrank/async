package main

import (
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

func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	var m sync.Mutex

	for num := range in {
		intNum := num.(int)

		dataConn := make(chan string, 1)

		wg.Add(2)

		go func(in <-chan interface{}, connector chan<- string, data string, w *sync.WaitGroup, m *sync.Mutex) {
			defer close(connector)
			defer w.Done()
			m.Lock()
			md5Data := DataSignerMd5(data) // Overheat protection
			m.Unlock()
			connector <- DataSignerCrc32(md5Data)
		}(in, dataConn, strconv.Itoa(intNum), &wg, &m)

		go func(in <-chan interface{}, connector <-chan string, out chan<- interface{}, w *sync.WaitGroup, n int) {
			defer w.Done()
			resultData := DataSignerCrc32(strconv.Itoa(n)) + "~" + (<-connector)
			out <- resultData
		}(in, dataConn, out, &wg, intNum)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var glWg sync.WaitGroup

	for hash := range in {
		var multiResult string

		resMultiHash := make([]string, 6)
		conChan := make(chan []string)

		var m sync.Mutex

		glWg.Add(1)
		go func(hash interface{}, conChan chan []string, glWg *sync.WaitGroup) {
			var pvtWg sync.WaitGroup
			pvtWg.Add(6)
			for i := 0; i < 6; i++ {
				go func(data string, i int, results []string, pwg *sync.WaitGroup, m *sync.Mutex) {
					defer pwg.Done()
					hash := DataSignerCrc32(strconv.Itoa(i) + data)
					m.Lock()
					results[i] = hash
					m.Unlock()
				}(hash.(string), i, resMultiHash, &pvtWg, &m)
			}
			pvtWg.Wait() // Waits until all 6 are created

			go func(con <-chan []string, wg *sync.WaitGroup) {
				defer glWg.Done()
				multiResult = strings.Join((<-con), "")
				out <- multiResult
			}(conChan, glWg)
			conChan <- resMultiHash

		}(hash, conChan, &glWg)

	}
	glWg.Wait() // Waits until all in-hashes are processed
}

func CombineResults(in, out chan interface{}) {
	var allMultiHash []string
	for multiHash := range in {
		allMultiHash = append(allMultiHash, multiHash.(string))
	}
	sort.Strings(allMultiHash)
	joinedResult := strings.Join(allMultiHash, "_")
	out <- joinedResult
}
