package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var poolCmd = &cobra.Command{
	Use:   "pool",
	Short: "Manage storage pools",
}

var poolListCmd = &cobra.Command{
	Use:   "list",
	Short: "List storage pools",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		buckets, err := client.ListBucketMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing pools: %w", err)
		}

		headers := []string{"NAME", "CREATED"}
		var rows [][]string
		for _, b := range buckets {
			created := time.Unix(0, b.CreationDate).UTC().Format("2006-01-02T15:04:05Z")
			rows = append(rows, []string{b.Name, created})
		}
		printTable(headers, rows)
		return nil
	},
}

var poolGetCmd = &cobra.Command{
	Use:   "get [name]",
	Short: "Get storage pool details",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		vol, err := client.GetVolumeMeta(ctx, args[0])
		if err != nil {
			return fmt.Errorf("getting pool %q: %w", args[0], err)
		}

		fmt.Printf("Pool:      %s\n", vol.Pool)
		fmt.Printf("Volume ID: %s\n", vol.VolumeID)
		fmt.Printf("Size:      %d bytes\n", vol.SizeBytes)
		fmt.Printf("Chunks:    %d\n", len(vol.ChunkIDs))
		return nil
	},
}

func init() {
	poolCmd.AddCommand(poolListCmd, poolGetCmd)
	rootCmd.AddCommand(poolCmd)
}
