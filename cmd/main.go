package main

import (
	"fmt"
	"os"

	sapling "github.com/KhaledMosaad/B-sapling"
)

func main() {
	fmt.Println(os.Getpagesize())

	_, err := sapling.Open("./db/fast.db")

	if err != nil {
		panic(err)
	}

}
