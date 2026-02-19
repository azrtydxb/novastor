// Package main provides the novastorctl CLI tool.
// This command-line interface allows operators to manage NovaStor storage pools,
// volumes, snapshots, and other cluster resources.
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
