package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
)

type Record struct {
	Index, OrgID, Name, Website, Country, Description, Founded, Industry, NumEmployees string
}

func (r *Record) String() string {
	return fmt.Sprintf("Index: %s, OrgID: %s, Name: %s, Website: %s, Country: %s, Description: %s, Founded: %s, Industry: %s, NumEmployees: %s",
		r.Index, r.OrgID, r.Name, r.Website, r.Country, r.Description, r.Founded, r.Industry, r.NumEmployees)
}

func main() {
	var inputFile string
	flag.StringVar(&inputFile, "input.file", "", "CSV input file")
	flag.Parse()

	if inputFile == "" {
		fmt.Println("No input file provided. Please use the 'input.file' flag to provide a CSV input file.")
		os.Exit(1)
	}

	records, err := readCSV(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading CSV: %v\n", err)
		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		mergeSort(records, 0, len(records)-1)
	}()
	wg.Wait()

	fmt.Println("Sorted records:")
	for _, record := range records {
		fmt.Println(record)
	}
}

func readCSV(file string) ([]*Record, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Read() // skip the headers
	var records []*Record

	for {
		line, err := r.Read()
		if err != nil {
			break
		}

		record := &Record{
			Index:        line[0],
			OrgID:        line[1],
			Name:         line[2],
			Website:      line[3],
			Country:      line[4],
			Description:  line[5],
			Founded:      line[6],
			Industry:     line[7],
			NumEmployees: line[8],
		}

		records = append(records, record)
	}

	return records, nil
}

func mergeSort(records []*Record, low int, high int) {
	if low < high {
		mid := low + (high-low)/2
		mergeSort(records, low, mid)
		mergeSort(records, mid+1, high)
		merge(records, low, mid, high)
	}
}

func merge(records []*Record, low int, mid int, high int) {
	left := append([]*Record{}, records[low:mid+1]...)
	right := append([]*Record{}, records[mid+1:high+1]...)

	i := 0
	j := 0
	k := low

	for i < len(left) && j < len(right) {
		if strings.Compare(left[i].Name, right[j].Name) <= 0 {
			records[k] = left[i]
			i++
		} else {
			records[k] = right[j]
			j++
		}
		k++
	}

	for i < len(left) {
		records[k] = left[i]
		i++
		k++
	}

	for j < len(right) {
		records[k] = right[j]
		j++
		k++
	}
}
