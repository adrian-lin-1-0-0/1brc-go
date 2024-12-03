package main

import (
	"1brc-go/lb"
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const input = "csv/weather_stations.csv"

func main() {
	file, err := os.Open(input)

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var keyCnt = make(map[string]int)

	balancer := lb.New(runtime.NumCPU(), nil)
	channels := balancer.GetChannels()
	maps := make([]map[string]float32, len(channels))

	for i := 0; i < len(channels); i++ {
		maps[i] = make(map[string]float32)
	}

	for i := 0; i < len(channels); i++ {
		go func(ch <-chan *lb.Data, i int) {
			for data := range ch {
				value, err := strconv.ParseFloat(strings.TrimSpace(data.Value), 64)
				if err != nil {
					log.Fatalf("invalid value: %s", data.Value)
				}
				maps[i][data.Key] += float32(value)
			}
		}(channels[i], i)
	}

	fileScanner := bufio.NewScanner(file)

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

		keyCnt[key]++
	}

	for i := 0; i < len(channels); i++ {
		close(channels[i])
	}

	keys := make([]string, 0, len(keyCnt))
	for k := range keyCnt {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//var output = os.Stdout
	var output bytes.Buffer
	writer := bufio.NewWriter(&output)

	for _, k := range keys {
		i := balancer.Hash([]byte(k)) % uint32(len(channels))
		_, err := writer.WriteString(fmt.Sprintf("%s: %.2f\n", k, maps[i][k]/float32(keyCnt[k])))
		if err != nil {
			log.Fatal(err)
		}
	}

}
