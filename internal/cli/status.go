package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return fmt.Errorf("connecting to metadata service: %w", err)
		}
		defer client.Close()

		ctx := context.Background()

		// Bucket count
		buckets, err := client.ListBucketMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing buckets: %w", err)
		}

		fmt.Println("=== NovaStor Cluster Status ===")
		fmt.Printf("Metadata endpoint: %s\n", metaAddr)
		fmt.Printf("Buckets:           %d\n", len(buckets))
		fmt.Println("Health:            OK (connected)")
		return nil
	},
}

func init() { rootCmd.AddCommand(statusCmd) }
