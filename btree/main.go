package main

import (
	"btree/btree"
	"btree/cli"
	"bufio"
	"os"
)

func main() {
	tree := btree.NewBTree()
	scanner := bufio.NewScanner(os.Stdin)
	demo := cli.NewCli(scanner, tree)
	demo.Start()
}
