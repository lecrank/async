package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const multiHashStepsAmount int = 6

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

// Calculates a hash for each step
func oneStepComputation(wg *sync.WaitGroup, data string, dest *string) {
	defer wg.Done()
	*dest = DataSignerCrc32(data)
}

// Fills the string slice with hashes for each step from 0 to 6
func hashesForAllSteps(hash interface{}, resMultiHash []string) {
	var wg sync.WaitGroup

	wg.Add(multiHashStepsAmount)
	for i := 0; i < multiHashStepsAmount; i++ {
		go oneStepComputation(&wg, strconv.Itoa(i)+hash.(string), &resMultiHash[i])
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup

	for hash := range in {
		conChan := make(chan []string)

		resMultiHash := make([]string, multiHashStepsAmount)

		wg.Add(1)
		go func(hash interface{}, resMultHash []string, conChan chan []string, glWg *sync.WaitGroup) {

			hashesForAllSteps(hash, resMultHash)

			go func(con <-chan []string, wg *sync.WaitGroup) {
				defer glWg.Done()
				multiResult := strings.Join((<-con), "")
				out <- multiResult
			}(conChan, glWg)

			conChan <- resMultHash

		}(hash, resMultiHash, conChan, &wg)

	}
	wg.Wait() // Waits until all in-hashes are processed
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
