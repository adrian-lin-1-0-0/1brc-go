package main

import (
	"1brc-go/stream"
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
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

	var data = make(map[string]*stream.Stream)

	fileScanner := bufio.NewScanner(file)

	for fileScanner.Scan() {
		line := fileScanner.Text()
		row := strings.Split(line, ";")
		if len(row) != 2 {
			log.Fatalf("invalid line: %s", line)
		}

		key := strings.TrimSpace(row[0])
		value, err := strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
		if err != nil {
			log.Fatalf("invalid value: %s", row[1])
		}

		if _, ok := data[key]; !ok {
			data[key] = stream.New()
		}
		data[key].Add(float64(value))

	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var output bytes.Buffer
	writer := bufio.NewWriter(&output)

	for _, k := range keys {
		v := data[k]
		_, err := writer.WriteString(fmt.Sprintf("%s: %.2f/%.2f/%.2f\n", k, v.Min(), v.Mean(), v.Max()))
		if err != nil {
			log.Fatal(err)
		}
	}

}
