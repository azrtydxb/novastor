package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

// jsonNode represents a storage node in JSON output format.
type jsonNode struct {
	NodeID            string `json:"nodeId"`
	Address           string `json:"address"`
	Status            string `json:"status"`
	DiskCount         int    `json:"diskCount"`
	TotalCapacity     int64  `json:"totalCapacity"`
	AvailableCapacity int64  `json:"availableCapacity"`
	LastHeartbeat     string `json:"lastHeartbeat"`
}

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Manage storage nodes",
}

var nodeListCmd = &cobra.Command{
	Use:   "list",
	Short: "List registered storage nodes",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes: %w", err)
		}

		if len(nodes) == 0 {
			if output == "json" {
				return json.NewEncoder(os.Stdout).Encode([]jsonNode{})
			}
			fmt.Println("No storage nodes registered.")
			return nil
		}

		headers := []string{"NODE ID", "ADDRESS", "STATUS", "DISKS", "TOTAL", "AVAILABLE", "LAST HEARTBEAT"}
		var rows [][]string
		var jsonNodes []jsonNode
		for _, n := range nodes {
			heartbeat := "never"
			if n.LastHeartbeat > 0 {
				heartbeat = time.Unix(n.LastHeartbeat, 0).UTC().Format("2006-01-02T15:04:05Z")
			}
			rows = append(rows, []string{
				n.NodeID,
				n.Address,
				n.Status,
				strconv.Itoa(n.DiskCount),
				formatBytes(n.TotalCapacity),
				formatBytes(n.AvailableCapacity),
				heartbeat,
			})
			jsonNodes = append(jsonNodes, jsonNode{
				NodeID:            n.NodeID,
				Address:           n.Address,
				Status:            n.Status,
				DiskCount:         n.DiskCount,
				TotalCapacity:     n.TotalCapacity,
				AvailableCapacity: n.AvailableCapacity,
				LastHeartbeat:     heartbeat,
			})
		}
		printTableOrJSON(headers, rows, jsonNodes)
		return nil
	},
}

var nodeGetCmd = &cobra.Command{
	Use:   "get [node-id]",
	Short: "Get storage node details",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
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

		heartbeat := "never"
		if node.LastHeartbeat > 0 {
			heartbeat = time.Unix(node.LastHeartbeat, 0).UTC().Format("2006-01-02T15:04:05Z")
		}

		fmt.Printf("Node ID:            %s\n", node.NodeID)
		fmt.Printf("Address:            %s\n", node.Address)
		fmt.Printf("Status:             %s\n", node.Status)
		fmt.Printf("Disks:              %d\n", node.DiskCount)
		fmt.Printf("Total Capacity:     %s\n", formatBytes(node.TotalCapacity))
		fmt.Printf("Available Capacity: %s\n", formatBytes(node.AvailableCapacity))
		fmt.Printf("Last Heartbeat:     %s\n", heartbeat)
		return nil
	},
}

func init() {
	nodeCmd.AddCommand(nodeListCmd, nodeGetCmd)
	rootCmd.AddCommand(nodeCmd)
}
