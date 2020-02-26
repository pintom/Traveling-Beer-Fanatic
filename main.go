package main

import "fmt"

func main() {
	hello("World")
}

func hello(w string) {
	fmt.Printf("Hello %s!", w)
}
