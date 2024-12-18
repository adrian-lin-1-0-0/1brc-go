package main

import (
	"1brc-go/stream"
	"io"
	"log"
	"runtime"
	"strconv"
	"sync"

	"golang.org/x/exp/mmap"
)

const input = "measurements.txt"
const chunkSize = 1024

func mapHandler(ch <-chan int, readerAt io.ReaderAt, wg *sync.WaitGroup) {
	defer wg.Done()
	m := make(map[string]*stream.Stream)
	defer func() {
		for k, v := range m {
			log.Printf("key: %s, min: %f, max: %f, mean: %f", k, v.Min(), v.Max(), v.Mean())
		}
	}()

	for offset := range ch {
		buf := make([]byte, chunkSize)
		_, err := readerAt.ReadAt(buf, int64(offset))
		if err != nil {
			return
		}
		k, l, j := 0, 0, 0
		for i := 0; i < chunkSize; i++ {
			if buf[i] == ';' {
				k = i
			} else if buf[i] == '\n' {
				l = i
				name := string(buf[j:k])
				if l-k < 2 {
					break
				}
				temp, err := strconv.ParseFloat(string(buf[k+1:l]), 64)
				if err != nil {
					log.Fatal("invalid temp: ", err)
				}
				if _, ok := m[name]; !ok {
					m[name] = stream.New()
				}
				m[name].Add(temp)
				j = l + 1
			}
		}
	}
}

func main() {

	readerAt, err := mmap.Open(input)
	if err != nil {
		log.Fatal(err)
	}
	defer readerAt.Close()
	wg := sync.WaitGroup{}
	channels := make([]chan int, runtime.NumCPU())

	for i := 0; i < runtime.NumCPU(); i++ {
		channels[i] = make(chan int)
		wg.Add(1)
		go mapHandler(channels[i], readerAt, &wg)
	}
	cnt := 0
	for i := 0; i < readerAt.Len(); i += chunkSize {
		channels[cnt%runtime.NumCPU()] <- i
		cnt++
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		close(channels[i])
	}

	wg.Wait()

}
