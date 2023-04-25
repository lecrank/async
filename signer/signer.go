package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const numberOfSteps int = 6

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

// Counts the first half of SingleHash
func countHalf(con chan<- string, n int, w *sync.WaitGroup, m *sync.Mutex) {
	strN := strconv.Itoa(n)
	defer close(con)
	defer w.Done()
	m.Lock()
	md5Data := DataSignerMd5(strN) // Overheat protection
	m.Unlock()
	con <- DataSignerCrc32(md5Data)
}

// Counts the second half of SingleHash
func countAnother(con <-chan string, out chan<- interface{}, n int, w *sync.WaitGroup) {
	defer w.Done()
	strN := strconv.Itoa(n)
	resultData := DataSignerCrc32(strN) + "~" + (<-con)
	out <- resultData
}

func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	var m sync.Mutex

	for num := range in {
		intNum := num.(int)

		dataConnector := make(chan string, 1)

		wg.Add(2)

		go countHalf(dataConnector, intNum, &wg, &m)
		go countAnother(dataConnector, out, intNum, &wg)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var glWg sync.WaitGroup

	for hash := range in {
		var multiResult string

		resMultiHash := make([]string, numberOfSteps)
		conChan := make(chan []string)

		glWg.Add(1)
		go func(hash interface{}, conChan chan []string, glWg *sync.WaitGroup) {

			var m sync.Mutex
			var pvtWg sync.WaitGroup
			pvtWg.Add(numberOfSteps)
			for i := 0; i < numberOfSteps; i++ {
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
