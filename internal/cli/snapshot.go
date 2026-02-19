package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/spf13/cobra"
)

// jsonSnapshot represents a snapshot in JSON output format.
type jsonSnapshot struct {
	SnapshotID     string   `json:"snapshotId"`
	SourceVolumeID string   `json:"sourceVolumeId"`
	SizeBytes      uint64   `json:"sizeBytes"`
	ChunkIDs       []string `json:"chunkIds"`
	ReadyToUse     bool     `json:"readyToUse"`
	Created        string   `json:"created"`
}

var snapshotCmd = &cobra.Command{
	Use:     "snapshot",
	Aliases: []string{"snap"},
	Short:   "Manage volume snapshots",
}

var snapshotListCmd = &cobra.Command{
	Use:   "list",
	Short: "List snapshots",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		snapshots, err := client.ListSnapshots(ctx)
		if err != nil {
			return fmt.Errorf("listing snapshots: %w", err)
		}

		if len(snapshots) == 0 {
			if output == "json" {
				return json.NewEncoder(os.Stdout).Encode([]jsonSnapshot{})
			}
			fmt.Println("No snapshots found.")
			return nil
		}

		headers := []string{"SNAPSHOT ID", "SOURCE VOLUME", "SIZE (BYTES)", "CHUNKS", "READY", "CREATED"}
		var rows [][]string
		var jsonSnaps []jsonSnapshot
		for _, s := range snapshots {
			created := time.Unix(0, s.CreationTime).UTC().Format("2006-01-02T15:04:05Z")
			rows = append(rows, []string{
				s.SnapshotID,
				s.SourceVolumeID,
				strconv.FormatUint(s.SizeBytes, 10),
				strconv.Itoa(len(s.ChunkIDs)),
				strconv.FormatBool(s.ReadyToUse),
				created,
			})
			jsonSnaps = append(jsonSnaps, jsonSnapshot{
				SnapshotID:     s.SnapshotID,
				SourceVolumeID: s.SourceVolumeID,
				SizeBytes:      s.SizeBytes,
				ChunkIDs:       s.ChunkIDs,
				ReadyToUse:     s.ReadyToUse,
				Created:        created,
			})
		}
		printTableOrJSON(headers, rows, jsonSnaps)
		return nil
	},
}

var snapshotGetCmd = &cobra.Command{
	Use:   "get [snapshot-id]",
	Short: "Get snapshot details",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		snap, err := client.GetSnapshot(ctx, args[0])
		if err != nil {
			return fmt.Errorf("getting snapshot %q: %w", args[0], err)
		}

		fmt.Printf("Snapshot ID:    %s\n", snap.SnapshotID)
		fmt.Printf("Source Volume:  %s\n", snap.SourceVolumeID)
		fmt.Printf("Size:           %d bytes\n", snap.SizeBytes)
		fmt.Printf("Chunks:         %d\n", len(snap.ChunkIDs))
		fmt.Printf("Ready to Use:   %t\n", snap.ReadyToUse)
		fmt.Printf("Created:        %s\n", time.Unix(0, snap.CreationTime).UTC().Format("2006-01-02T15:04:05Z"))
		return nil
	},
}

var snapshotCreateVolumeID string

var snapshotCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a snapshot from a volume",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()

		// Retrieve the source volume to capture its current chunk list and size.
		vol, err := client.GetVolumeMeta(ctx, snapshotCreateVolumeID)
		if err != nil {
			return fmt.Errorf("getting source volume %q: %w", snapshotCreateVolumeID, err)
		}

		snapID := uuid.New().String()
		chunksCopy := make([]string, len(vol.ChunkIDs))
		copy(chunksCopy, vol.ChunkIDs)

		meta := &metadata.SnapshotMeta{
			SnapshotID:     snapID,
			SourceVolumeID: vol.VolumeID,
			SizeBytes:      vol.SizeBytes,
			ChunkIDs:       chunksCopy,
			CreationTime:   time.Now().UnixNano(),
			ReadyToUse:     true,
		}

		if err := client.PutSnapshot(ctx, meta); err != nil {
			return fmt.Errorf("creating snapshot: %w", err)
		}

		fmt.Printf("Snapshot created: %s\n", snapID)
		return nil
	},
}

var snapshotDeleteCmd = &cobra.Command{
	Use:   "delete [snapshot-id]",
	Short: "Delete a snapshot",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		if err := client.DeleteSnapshot(ctx, args[0]); err != nil {
			return fmt.Errorf("deleting snapshot %q: %w", args[0], err)
		}

		fmt.Printf("Snapshot deleted: %s\n", args[0])
		return nil
	},
}

func init() {
	snapshotCreateCmd.Flags().StringVar(&snapshotCreateVolumeID, "volume-id", "", "Source volume ID (required)")
	_ = snapshotCreateCmd.MarkFlagRequired("volume-id")

	snapshotCmd.AddCommand(snapshotListCmd, snapshotGetCmd, snapshotCreateCmd, snapshotDeleteCmd)
	rootCmd.AddCommand(snapshotCmd)
}
