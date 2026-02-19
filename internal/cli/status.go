package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// jsonStatus represents the cluster status in JSON output format.
type jsonStatus struct {
	MetadataEndpoint  string `json:"metadataEndpoint"`
	Nodes             int    `json:"nodes"`
	ReadyNodes        int    `json:"readyNodes"`
	Volumes           int    `json:"volumes"`
	Snapshots         int    `json:"snapshots"`
	Buckets           int    `json:"buckets"`
	TotalCapacity     int64  `json:"totalCapacity"`
	AvailableCapacity int64  `json:"availableCapacity"`
	Health            string `json:"health"`
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	RunE: func(_ *cobra.Command, _ []string) error {
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

		if output == "json" {
			status := jsonStatus{
				MetadataEndpoint:  metaAddr,
				Nodes:             len(nodes),
				ReadyNodes:        readyNodes,
				Volumes:           len(volumes),
				Snapshots:         len(snapshots),
				Buckets:           len(buckets),
				TotalCapacity:     totalCap,
				AvailableCapacity: availCap,
				Health:            "OK",
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(status)
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
