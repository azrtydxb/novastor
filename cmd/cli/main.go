// Package main provides the novastorctl CLI tool.
// This command-line interface allows operators to manage NovaStor storage pools,
// volumes, snapshots, and other cluster resources.
package main

import (
	"os"

	"github.com/azrtydxb/novastor/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
