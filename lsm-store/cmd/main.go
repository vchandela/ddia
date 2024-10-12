package main

import (
	"fmt"
	"lsm/db"
)

func main() {
	// // test skip list
	// scanner := bufio.NewScanner(os.Stdin)
	// sl := skiplist.NewSkipList()
	// cli := cli.NewCLI(scanner, sl)
	// cli.Start()

	// test lsm (memtable only)
	key := []byte("hello")
	val := []byte("world")

	d := db.Open()
	d.Set(key, val)
	val1, err := d.Get(key)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Found val:%s for key:%s\n", string(val1), string(key))
	}
	d.Delete(key)
	val1, err = d.Get(key)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Found val:%s for key:%s\n", string(val1), string(key))
	}
}
