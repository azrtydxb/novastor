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
	fmt.Fprintf(os.Stderr, "novastor-agent %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "Agent not yet implemented")
}
