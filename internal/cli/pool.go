package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
)

var poolCmd = &cobra.Command{
	Use:   "pool",
	Short: "Manage storage pools",
	Long:  "View storage pool information derived from registered storage nodes.",
}

var poolListCmd = &cobra.Command{
	Use:   "list",
	Short: "List storage pools (node capacity overview)",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes for pool overview: %w", err)
		}

		if len(nodes) == 0 {
			fmt.Println("No storage nodes registered.")
			return nil
		}

		// Aggregate capacity across all nodes for a pool-level summary.
		var totalCapacity, availableCapacity int64
		readyCount := 0
		for _, n := range nodes {
			totalCapacity += n.TotalCapacity
			availableCapacity += n.AvailableCapacity
			if n.Status == "ready" {
				readyCount++
			}
		}

		headers := []string{"NODES", "READY", "TOTAL CAPACITY", "AVAILABLE", "USED %"}
		usedPct := "0.0"
		if totalCapacity > 0 {
			pct := float64(totalCapacity-availableCapacity) / float64(totalCapacity) * 100
			usedPct = strconv.FormatFloat(pct, 'f', 1, 64)
		}
		rows := [][]string{
			{
				strconv.Itoa(len(nodes)),
				strconv.Itoa(readyCount),
				formatBytes(totalCapacity),
				formatBytes(availableCapacity),
				usedPct + "%",
			},
		}
		printTable(headers, rows)
		return nil
	},
}

var poolGetCmd = &cobra.Command{
	Use:   "get [node-id]",
	Short: "Get storage details for a specific node",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		node, err := client.GetNodeMeta(ctx, args[0])
		if err != nil {
			return fmt.Errorf("getting node %q: %w", args[0], err)
		}

		fmt.Printf("Node ID:            %s\n", node.NodeID)
		fmt.Printf("Address:            %s\n", node.Address)
		fmt.Printf("Status:             %s\n", node.Status)
		fmt.Printf("Disks:              %d\n", node.DiskCount)
		fmt.Printf("Total Capacity:     %s\n", formatBytes(node.TotalCapacity))
		fmt.Printf("Available Capacity: %s\n", formatBytes(node.AvailableCapacity))
		return nil
	},
}

func init() {
	poolCmd.AddCommand(poolListCmd, poolGetCmd)
	rootCmd.AddCommand(poolCmd)
}
