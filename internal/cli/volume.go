package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/google/uuid"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/spf13/cobra"
)

// jsonVolume represents a volume in JSON output format.
type jsonVolume struct {
	VolumeID  string   `json:"volumeId"`
	Pool      string   `json:"pool"`
	SizeBytes uint64   `json:"sizeBytes"`
	ChunkIDs  []string `json:"chunkIds"`
}

var volumeCmd = &cobra.Command{
	Use:     "volume",
	Aliases: []string{"vol"},
	Short:   "Manage block volumes",
}

var volumeListCmd = &cobra.Command{
	Use:   "list",
	Short: "List volumes",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		volumes, err := client.ListVolumesMeta(ctx)
		if err != nil {
			return fmt.Errorf("listing volumes: %w", err)
		}

		if len(volumes) == 0 {
			if output == "json" {
				return json.NewEncoder(os.Stdout).Encode([]jsonVolume{})
			}
			fmt.Println("No volumes found.")
			return nil
		}

		headers := []string{"VOLUME ID", "POOL", "SIZE (BYTES)", "CHUNKS"}
		var rows [][]string
		var jsonVols []jsonVolume
		for _, v := range volumes {
			rows = append(rows, []string{
				v.VolumeID,
				v.Pool,
				strconv.FormatUint(v.SizeBytes, 10),
				strconv.Itoa(len(v.ChunkIDs)),
			})
			jsonVols = append(jsonVols, jsonVolume{
				VolumeID:  v.VolumeID,
				Pool:      v.Pool,
				SizeBytes: v.SizeBytes,
				ChunkIDs:  v.ChunkIDs,
			})
		}
		printTableOrJSON(headers, rows, jsonVols)
		return nil
	},
}

var volumeGetCmd = &cobra.Command{
	Use:   "get [volume-id]",
	Short: "Get volume details",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		vol, err := client.GetVolumeMeta(ctx, args[0])
		if err != nil {
			return fmt.Errorf("getting volume %q: %w", args[0], err)
		}

		fmt.Printf("Volume ID: %s\n", vol.VolumeID)
		fmt.Printf("Pool:      %s\n", vol.Pool)
		fmt.Printf("Size:      %d bytes\n", vol.SizeBytes)
		fmt.Printf("Chunks:    %d\n", len(vol.ChunkIDs))
		return nil
	},
}

var (
	volumeCreatePool string
	volumeCreateSize string
)

var volumeCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new volume",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		sizeBytes, err := strconv.ParseUint(volumeCreateSize, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid size %q: %w", volumeCreateSize, err)
		}

		id := uuid.New().String()
		meta := &metadata.VolumeMeta{
			VolumeID:  id,
			Pool:      volumeCreatePool,
			SizeBytes: sizeBytes,
		}

		ctx := context.Background()
		if err := client.PutVolumeMeta(ctx, meta); err != nil {
			return fmt.Errorf("creating volume: %w", err)
		}

		fmt.Printf("Volume created: %s\n", id)
		return nil
	},
}

var volumeDeleteCmd = &cobra.Command{
	Use:   "delete [volume-id]",
	Short: "Delete a volume",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		if err := client.DeleteVolumeMeta(ctx, args[0]); err != nil {
			return fmt.Errorf("deleting volume %q: %w", args[0], err)
		}

		fmt.Printf("Volume deleted: %s\n", args[0])
		return nil
	},
}

func init() {
	volumeCreateCmd.Flags().StringVar(&volumeCreatePool, "pool", "", "Storage pool name (required)")
	volumeCreateCmd.Flags().StringVar(&volumeCreateSize, "size", "", "Volume size in bytes (required)")
	_ = volumeCreateCmd.MarkFlagRequired("pool")
	_ = volumeCreateCmd.MarkFlagRequired("size")

	volumeCmd.AddCommand(volumeListCmd, volumeGetCmd, volumeCreateCmd, volumeDeleteCmd)
	rootCmd.AddCommand(volumeCmd)
}
