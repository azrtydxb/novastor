package main

import (
	"os"

	"github.com/piwi3910/novastor/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
