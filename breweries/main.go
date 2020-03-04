package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
	"log"
	"os"
	"strconv"
)

func main() {
	fmt.Println("Uploading breweries to the database...")
	// Create new dgraph client
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	dgraphClient := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	// Set up the schema
	op := &api.Operation{
		Schema: `
			id: int @index(int) .
			name: string @index(exact) .
			location: geo @index(geo) .
			beers: [string] .
		`,
	}
	err = dgraphClient.Alter(context.Background(), op)
	if err != nil {
		log.Fatal(err)
	}

	// Open CSVs
	rawGeocodes := openCSV("./breweries/geocodes.csv")
	rawBreweries := openCSV("./breweries/breweries.csv")
	rawBeers := openCSV("./breweries/beers.csv")

	type Location struct {
		Type        string    `json:"type,omitempty"`
		Coordinates []float64 `json:"coordinates,omitempty"`
	}

	type Brewery struct {
		ID       int      `json:"id,omitempty"`
		Name     string   `json:"name,omitempty"`
		Location Location `json:"location,omitempty"`
		Beers    []string `json:"beers,omitempty"`
	}

	var counter int
	// Go through the tables and find breweries coordinates and beer types. Add them to the database.
	for _, geo := range rawGeocodes {
		for _, brew := range rawBreweries {
			if geo[1] == brew[0] {
				var beers = make([]string, 0)
				for _, beer := range rawBeers {
					if beer[1] == geo[1] {
						beers = append(beers, beer[2])
					}
				}

				id, err := strconv.Atoi(geo[1])
				if err != nil {
					log.Fatal(err)
				}
				lat, err := strconv.ParseFloat(geo[2], 64)
				if err != nil {
					log.Fatal(err)
				}
				long, err := strconv.ParseFloat(geo[3], 64)
				if err != nil {
					log.Fatal(err)
				}

				var br = Brewery{
					id,
					brew[1],
					Location{"Point", []float64{lat, long}},
					beers,
				}

				// dgraph
				txn := dgraphClient.NewTxn()

				pb, err := json.Marshal(br)
				if err != nil {
					log.Fatal(err)
				}
				//fmt.Println(string(pb))
				mu := &api.Mutation{
					SetJson: pb,
				}
				req := &api.Request{CommitNow: true, Mutations: []*api.Mutation{mu}}
				_, err = txn.Do(context.Background(), req)
				if err != nil {
					log.Fatal(err)
				}

				txn.Discard(context.Background())
				//fmt.Printf("%v\n", br)

				counter++
				fmt.Printf("\r%v", counter)
			}
		}
	}

	fmt.Println(" been uploaded. Done!")
}

func openCSV(name string) [][]string {
	// Open the file
	b, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	// Create new CSV reader
	csvReader := csv.NewReader(b)
	csvReader.FieldsPerRecord = -1 // each row can have variable number of fields
	rawCSV, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	return rawCSV
}
