// Package cli provides the novastorctl command-line interface for NovaStor.
// This package implements commands for managing storage pools, volumes,
// snapshots, buckets, and cluster resources.
package cli

import "github.com/spf13/cobra"

var (
	metaAddr string
	output   string
	rootCmd  = &cobra.Command{
		Use:   "novastorctl",
		Short: "NovaStor CLI - manage your unified storage cluster",
		Long:  "novastorctl is the command-line interface for managing NovaStor storage pools, volumes, filesystems, and object stores.",
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&metaAddr, "meta-addr", "localhost:7001", "Metadata service gRPC address")
	rootCmd.PersistentFlags().StringVarP(&output, "output", "o", "", "Output format (json)")
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}
