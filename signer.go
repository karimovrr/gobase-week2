package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

const th = 6

var hashInsert = func(ch chan string, s string, fnc func(string) string) {
	result := fnc(s)
	ch <- result
}

func singleHashProcess(wg *sync.WaitGroup, d, m string, ch chan interface{}) {
	defer wg.Done()
	chan32 := make(chan string)
	chanMd5 := make(chan string)
	go hashInsert(chan32, d, DataSignerCrc32)
	go hashInsert(chanMd5, m, DataSignerCrc32)
	crc32Hash := <-chan32
	crc32Md5Hash := <-chanMd5
	ch <- crc32Hash + "~" + crc32Md5Hash
}

func SingleHash(in, out chan interface{}) {
	var wgroup sync.WaitGroup
	for i := range in {
		data := fmt.Sprintf("%v", i)
		crcMd5 := DataSignerMd5(data)
		wgroup.Add(1)
		go singleHashProcess(&wgroup, data, crcMd5, out)
	}
	wgroup.Wait()
}

func addToMultiHash(wg *sync.WaitGroup, s string, arr []string, i int) {
	defer wg.Done()
	hash := DataSignerCrc32(s)
	arr[i] = hash
}

func multiHashProcess(wgParams *sync.WaitGroup, s interface{}, ch chan interface{}) {
	var wg sync.WaitGroup
	arr := make([]string, th)
	defer wgParams.Done()
	for i := 0; i < th; i++ {
		wg.Add(1)
		data := fmt.Sprintf("%v%v", i, s)
		go addToMultiHash(&wg, data, arr, i)
	}
	wg.Wait()
	hash := strings.Join(arr, "")
	ch <- hash
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	for i := range in {
		wg.Add(1)
		go multiHashProcess(&wg, i, out)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var arr []string
	for i := range in {
		arr = append(arr, i.(string))
	}
	sort.Strings(arr)
	res := strings.Join(arr, "_")
	out <- res
}

func pipelineProccess(wg *sync.WaitGroup, workFunc job, in, out chan interface{}) {
	defer wg.Done()
	defer close(out)
	workFunc(in, out)
}

func ExecutePipeline(works ...job) {
	var wgroup sync.WaitGroup
	in := make(chan interface{})
	for _, workFunc := range works {
		wgroup.Add(1)
		out := make(chan interface{})
		go pipelineProccess(&wgroup, workFunc, in, out)
		in = out
	}
	wgroup.Wait()
}
