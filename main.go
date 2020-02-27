package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
	"log"
	"math"
	"time"
)

var (
	dgraph = "127.0.0.1:9080"
	lat    = flag.Float64("lat", 54.900518, "Latitude of initial location")
	long   = flag.Float64("long", 23.893718, "Longitude of initial location")
)

func main() {
	startTimer := time.Now()
	flag.Parse()

	resp := queryBreweries(*lat, *long)
	var beerTypes []string
	var totalDistance float64
	fmt.Printf("Found %d beer factories:\n", len(resp))

	fmt.Printf("\t -> HOME: %g %g distance 0km\n", *lat, *long)
	for _, v := range resp {
		beerTypes = append(beerTypes, v.Beers...)

		// Calculate the distance
		lat1 := v.Location.Coordinates[0]
		long1 := v.Location.Coordinates[1]
		distance := findDistance(*lat, *long, lat1, long1)
		totalDistance += distance

		fmt.Printf("\t -> [%d] %s: %g %g distance %.fkm\n", v.ID, v.Name, lat1,
			long1, distance)
	}
	fmt.Printf("\t <- HOME: %g %g distance 0km\n", *lat, *long)

	fmt.Printf("\nTotal distance travelled: %.fkm\n", totalDistance)

	fmt.Printf("\nCollected %d beer types:\n", len(beerTypes))
	for _, v := range beerTypes {
		fmt.Printf("\t -> %s\n", v)
	}

	// How long the program took to complete the task.
	timeLasted := time.Since(startTimer)
	fmt.Printf("\nProgram took: %s\n", timeLasted)
}

// findDistance returns distance in kilometers between two points on earth using Haversin formula.
func findDistance(lat1, long1, lat2, long2 float64) float64 {
	// convert to radians
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = long1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = long2 * math.Pi / 180
	r = 6371009 // earth radius in meters

	h := haversin(la2-la1) + math.Cos(la1)*math.Cos(la2)*haversin(lo2-lo1)

	return (2 * r * math.Asin(math.Sqrt(h))) / 1000
}

// Haversin function.
func haversin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

type brewery struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Location struct {
		Coordinates []float64 `json:"coordinates"`
	} `json:"location"`
	Beers []string `json:"beers"`
}

// Returns a list of breweries ir range of 1000km of given coordinates.
func queryBreweries(lat float64, long float64) []brewery {
	conn, err := grpc.Dial(dgraph, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	var q = fmt.Sprintf(`
	{
		breweries(func: near(location, [%v, %v], 1000000) ) {
			id
			name
			location
			beers
		}
	}
	
	`, lat, long)

	resp, err := dg.NewTxn().Query(context.Background(), q)

	if err != nil {
		log.Fatal(err)
	}

	var decode struct {
		Breweries []brewery
	}

	if err := json.Unmarshal(resp.GetJson(), &decode); err != nil {
		log.Fatal(err)
	}

	return decode.Breweries
}
