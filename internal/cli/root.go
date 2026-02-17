package cli

import "github.com/spf13/cobra"

var (
	metaAddr string
	rootCmd  = &cobra.Command{
		Use:   "novactl",
		Short: "NovaStor CLI - manage your unified storage cluster",
		Long:  "novactl is the command-line interface for managing NovaStor storage pools, volumes, filesystems, and object stores.",
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&metaAddr, "meta-addr", "localhost:7001", "Metadata service gRPC address")
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}
