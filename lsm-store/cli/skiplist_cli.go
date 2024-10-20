package cli

import (
	"bufio"
	"fmt"
	"lsm/skiplist"
	"os"
	"strings"
)

type SCLI struct {
	scanner  *bufio.Scanner
	skipList *skiplist.SkipList
}

func NewSCLI(s *bufio.Scanner, sl *skiplist.SkipList) *SCLI {
	return &SCLI{s, sl}
}

func (c *SCLI) Start() {
	c.printHelp()
	c.printPrompt()
	for {
		if c.scanner.Scan() {
			c.processInput(c.scanner.Text())
		}
	}
}

func (c *SCLI) printHelp() {
	fmt.Println(`
SkipList CLI

Available Commands:
  SET <key> <val> Insert a key-value pair into the SkipList
  DEL <key>       Remove a key-value pair from the SkipList
  GET <key>       Retrieve the value for key from the SkipList
  EXIT            Terminate this session
`)
}

func (c *SCLI) printPrompt() {
	fmt.Print("> ")
}

func (c *SCLI) processInput(line string) {
	fields := strings.Fields(line)

	if len(fields) < 1 {
		return
	}
	command := strings.ToLower(fields[0])

	switch command {
	default:
		fmt.Printf("Unknown command \"%s\"\n", command)
	case "set":
		c.processSetCommand(fields[1:])
	case "del":
		c.processDeleteCommand(fields[1:])
	case "get":
		c.processGetCommand(fields[1:])
	case "exit":
		os.Exit(0)
	}
	c.printPrompt()
}

func (c *SCLI) processSetCommand(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: SET <key> <value>")
		return
	}
	c.skipList.Insert([]byte(args[0]), []byte(args[1]))
	fmt.Println(c.skipList)
}

func (c *SCLI) processDeleteCommand(args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: DEL <key>")
		return
	}
	res := c.skipList.Delete([]byte(args[0]))

	if !res {
		fmt.Println("Key not found.")
		return
	}
	fmt.Println(c.skipList)
}

func (c *SCLI) processGetCommand(args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: GET <key>")
		return
	}
	val, found := c.skipList.Get([]byte(args[0]))

	if !found {
		fmt.Println("Key not found.")
		return
	}
	fmt.Println(string(val))
}
