package main

import (
	"fmt"
	"os"

	sapling "github.com/KhaledMosaad/B-sapling"
)

func main() {
	fmt.Println(os.Getpagesize())

	db, err := sapling.Open("./local/fast.db")

	if err != nil {
		panic(err)
	}

	defer db.Close()

	_, _, err = db.Upsert([]byte("My key 165"), []byte("My val 165"))
	if err != nil {
		panic(err)
	}

	val, err := db.Find([]byte("My key 165"))

	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))

}
