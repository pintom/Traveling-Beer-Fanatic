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
	dgraph          = "127.0.0.1:9080"
	lat             = flag.Float64("lat", 54.900518, "Latitude of initial geoLocation")
	long            = flag.Float64("long", 23.893718, "Longitude of initial geoLocation")
	totalDistanceKm = distance(*flag.Float64("fuel", 2000,
		"The distance that can be traveled before returning to the initial geoLocation"))
)

func main() {
	startTimer := time.Now()
	flag.Parse()

	breweries := queryBreweries(*lat, *long, totalDistanceKm/2) // there must be enough fuel for a return trip.

	err := breweries.findDistances()
	if err != nil {
		fmt.Println(err)
		return
	}

	cli(breweries, startTimer)

}

func cli(breweries breweries, startTimer time.Time) {
	fmt.Printf("Found %d beer factories:\n", len(breweries.orderedList))

	fmt.Printf("\t -> HOME: %g %g distance 0km\n", *lat, *long)

	for _, v := range breweries.orderedList {
		fmt.Printf("\t -> [%d] %s: %g %g distance %s\n", v.ID, v.Name, v.loc.lat, v.loc.long, v.distanceFromPrevious)
	}

	fmt.Printf("\t <- HOME: %g %g distance %s\n", *lat, *long, breweries.orderedList[len(breweries.orderedList)-1].distanceToHome)

	fmt.Printf("\nTotal distance travelled: %s\n", breweries.totalDistance)

	fmt.Printf("\nCollected %d beer types:\n", len(breweries.beerTypes))
	for _, v := range breweries.beerTypes {
		fmt.Printf("\t -> %s\n", v)
	}

	// How long the program took to complete the task.
	timeLasted := time.Since(startTimer)
	fmt.Printf("\nProgram took: %s\n", timeLasted)
}

type breweries struct {
	startingPoint geoLocation
	radius        distance
	list          map[int]*brewery
	orderedList   []*brewery
	totalDistance distance
	beerTypes     []string
}

type brewery struct {
	ID    int
	Name  string
	loc   geoLocation
	Beers []string

	distanceToHome       distance
	distanceFromPrevious distance
}

func (b *breweries) findDistances() error {
	if len(b.list) < 1 {
		return fmt.Errorf("sorry, no breweries found")
	}
	var closest distance = 99999
	b.orderedList = make([]*brewery, 1)
	// Find which one  is closest to the starting point.
	for _, v := range b.list {
		d := v.loc.distanceTo(b.startingPoint)
		//fmt.Println(d)
		if d < closest {
			closest = d
			b.orderedList[0] = v
			b.orderedList[0].distanceFromPrevious = d
		}
	}
	b.totalDistance += closest
	b.beerTypes = append(b.beerTypes, b.orderedList[0].Beers...)

	delete(b.list, b.orderedList[0].ID)

	for b.totalDistance+b.orderedList[len(b.orderedList)-1].distanceToHome < totalDistanceKm {

		closest = 999999
		br := brewery{}
		// find the next closest factory
		for _, v := range b.list {
			d := v.loc.distanceTo(b.orderedList[len(b.orderedList)-1].loc)

			if d < closest {
				closest = d
				br = *v
				br.distanceFromPrevious = d
				br.distanceToHome = v.loc.distanceTo(b.startingPoint)
			}
		}

		// if total distance is more than total possible range, do not add to the list
		if b.totalDistance+br.distanceToHome+closest > totalDistanceKm {
			break
		}
		b.totalDistance += closest
		b.orderedList = append(b.orderedList, &br)
		b.beerTypes = append(b.beerTypes, br.Beers...)
		delete(b.list, b.orderedList[len(b.orderedList)-1].ID)

	}

	b.totalDistance += b.orderedList[len(b.orderedList)-1].distanceToHome

	return nil
}

// Returns a list of breweries in range of given distance (in kilometers) and starting coordinates.
func queryBreweries(lat float64, long float64, radius distance) breweries {
	conn, err := grpc.Dial(dgraph, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	var q = fmt.Sprintf(`
	{
  		breweries(func: near(location, [%v, %v], %d) )  {
   			id
    		name
    		location
    		beers
  	}
}
	
	`, lat, long, int(radius*1000)) // radius has to be converted to meters

	resp, err := dg.NewTxn().Query(context.Background(), q)
	if err != nil {
		log.Fatal(err)
	}

	var decode struct {
		Breweries []struct {
			ID       int    `json:"id"`
			Name     string `json:"name"`
			Location struct {
				Type        string    `json:"type"`
				Coordinates []float64 `json:"coordinates"`
			} `json:"location"`
			Beers []string `json:"beers"`
		} `json:"breweries"`
	}
	//fmt.Printf("%s", resp.Json)

	if err := json.Unmarshal(resp.GetJson(), &decode); err != nil {
		log.Fatal(err)
	}

	//fmt.Println("returned breweries count: ", len(decode.Breweries))

	brewMap := make(map[int]*brewery)
	for _, v := range decode.Breweries {
		brewMap[v.ID] = &brewery{
			ID:   v.ID,
			Name: v.Name,
			loc: geoLocation{
				lat:  v.Location.Coordinates[0],
				long: v.Location.Coordinates[1],
			},
			Beers: v.Beers,
		}
	}

	return breweries{startingPoint: geoLocation{lat, long}, radius: distance(radius), list: brewMap}
}

// distance holds distance in kilometers.
type distance int

func (d distance) String() string { return fmt.Sprintf("%dkm", d) }

type geoLocation struct {
	lat  float64
	long float64
}

// distanceTo returns distance in kilometers between two points on earth using Haversin formula.
func (g geoLocation) distanceTo(loc geoLocation) distance {
	// convert to radians
	var la1, lo1, la2, lo2, r float64
	la1 = g.lat * math.Pi / 180
	lo1 = g.long * math.Pi / 180
	la2 = loc.lat * math.Pi / 180
	lo2 = loc.long * math.Pi / 180
	r = 6371009 // earth radius in meters

	h := haversin(la2-la1) + math.Cos(la1)*math.Cos(la2)*haversin(lo2-lo1)

	return distance((2 * r * math.Asin(math.Sqrt(h))) / 1000)
}

// Haversin function.
func haversin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}
