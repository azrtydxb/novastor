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

		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes: %w", err)
		}

		volumes, err := client.ListVolumesMeta(ctx)
		if err != nil {
			return fmt.Errorf("listing volumes: %w", err)
		}

		snapshots, err := client.ListSnapshots(ctx)
		if err != nil {
			return fmt.Errorf("listing snapshots: %w", err)
		}

		buckets, err := client.ListBucketMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing buckets: %w", err)
		}

		readyNodes := 0
		var totalCap, availCap int64
		for _, n := range nodes {
			if n.Status == "ready" {
				readyNodes++
			}
			totalCap += n.TotalCapacity
			availCap += n.AvailableCapacity
		}

		fmt.Println("=== NovaStor Cluster Status ===")
		fmt.Printf("Metadata endpoint: %s\n", metaAddr)
		fmt.Printf("Nodes:             %d (%d ready)\n", len(nodes), readyNodes)
		fmt.Printf("Volumes:           %d\n", len(volumes))
		fmt.Printf("Snapshots:         %d\n", len(snapshots))
		fmt.Printf("Buckets:           %d\n", len(buckets))
		if totalCap > 0 {
			fmt.Printf("Total Capacity:    %s\n", formatBytes(totalCap))
			fmt.Printf("Available:         %s\n", formatBytes(availCap))
		}
		fmt.Println("Health:            OK (connected)")
		return nil
	},
}

func init() { rootCmd.AddCommand(statusCmd) }
