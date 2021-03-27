package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	in := make(chan interface{}, 4)
	go func() {
		for i := 0; i < 1000; i++ {
			in <- i
		}
	}()

	out := make(chan interface{})
	f := func(in interface{}) interface{} {
		return in.(int) * 2
	}

	ProcessDataConcurrently(in, out, f, ConcurrencyOptions{
		InputBufferSize:  5,
		OutputBufferSize: 5,
		Concurrency:      5,
	})
	go func() {
		for result := range out {
			fmt.Println(result)
		}
	}()

	time.Sleep(20 * time.Second)
}

type request struct {
	order uint64
	val   interface{}
}

type ConcurrencyOptions struct {
	InputBufferSize  int
	OutputBufferSize int
	Concurrency      int
}

func ProcessDataConcurrently(in chan interface{}, out chan interface{}, workFn func(in interface{}) interface{}, options ConcurrencyOptions) {

	requestsChan := make(chan *request, options.InputBufferSize)
	go func() {
		defer func() {
			close(requestsChan)
		}()
		var order uint64 = 0
		for req := range in {
			requestsChan <- &request{order: order, val: req}
			order++
		}
	}()

	collectMap := make(map[uint64]*request)
	collectChan := make(chan *request, options.OutputBufferSize)

	var wg sync.WaitGroup
	for i := 0; i < options.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for request := range requestsChan {
				result := workFn(request.val)
				request.val = result
				collectChan <- request
			}
		}()
	}
	go func() {
		wg.Wait()
		close(collectChan)
	}()

	// collectMap result
	go func() {
		defer close(out)
		var current uint64 = 0
		for outItem := range collectChan {
			collectMap[outItem.order] = outItem
			if outItem, ok := collectMap[current]; ok {
				out <- outItem.val
				delete(collectMap, current)
				current++
			}
		}
		for len(collectMap) != 0 {
			if outItem, ok := collectMap[current]; ok {
				out <- outItem.val
				delete(collectMap, current)
				current++
			}
		}

	}()
}
