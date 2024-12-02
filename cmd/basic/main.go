package main

import (
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

	var data = make(map[string]float32)
	var keyCnt = make(map[string]int)

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

		data[key] += float32(value)
		keyCnt[key]++
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var output bytes.Buffer
	writer := bufio.NewWriter(&output)

	for _, k := range keys {
		_, err := writer.WriteString(fmt.Sprintf("%s: %.2f\n", k, data[k]/float32(keyCnt[k])))
		if err != nil {
			log.Fatal(err)
		}
	}

}
