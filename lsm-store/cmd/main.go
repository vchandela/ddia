package main

import (
	"bufio"
	"lsm/cli"
	"lsm/skiplist"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	sl := skiplist.NewSkipList()
	cli := cli.NewCLI(scanner, sl)
	cli.Start()
}
