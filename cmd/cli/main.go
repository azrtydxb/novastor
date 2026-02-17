package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novactl %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "CLI not yet implemented")
}
