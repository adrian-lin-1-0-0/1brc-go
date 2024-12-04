package main

import (
	"1brc-go/lb"
	"1brc-go/stream"
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const input = "csv/weather_stations.csv"

func main() {
	file, err := os.Open(input)

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	balancer := lb.New(runtime.NumCPU(), nil, 100)
	channels := balancer.GetChannels()
	maps := make([]map[string]*stream.Stream, len(channels))

	for i := 0; i < len(channels); i++ {
		maps[i] = make(map[string]*stream.Stream)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(channels))

	for i := 0; i < len(channels); i++ {
		go func(ch <-chan *lb.Data, i int) {
			defer wg.Done()
			for data := range ch {
				value, err := strconv.ParseFloat(strings.TrimSpace(data.Value), 64)
				if err != nil {
					log.Fatalf("invalid value: %s", data.Value)
				}

				if v, ok := maps[i][data.Key]; !ok {
					v := stream.New()
					maps[i][data.Key] = v
					v.Add(float64(value))
				} else {
					v.Add(float64(value))
				}
			}
		}(channels[i], i)
	}

	fileScanner := bufio.NewScanner(file)

	var keySets = make(map[string]struct{})

	for fileScanner.Scan() {
		line := fileScanner.Text()
		row := strings.Split(line, ";")

		if len(row) != 2 {
			log.Fatalf("invalid line: %s", line)
		}

		key := strings.TrimSpace(row[0])

		balancer.Handle(&lb.Data{
			Key:   key,
			Value: row[1],
		})

		keySets[key] = struct{}{}
	}

	for i := 0; i < len(channels); i++ {
		close(channels[i])
	}

	wg.Wait()

	keys := make([]string, 0, len(keySets))
	for k := range keySets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//var output = os.Stdout
	var output bytes.Buffer
	writer := bufio.NewWriter(&output)

	for _, k := range keys {
		i := balancer.Hash([]byte(k)) % uint32(len(channels))
		v := maps[i][k]
		_, err := writer.WriteString(fmt.Sprintf("%s: %.2f/%.2f/%.2f\n", k, v.Min(), v.Mean(), v.Max()))
		if err != nil {
			log.Fatal(err)
		}
	}

}
