package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/spf13/cobra"
)

// HealStatus represents the overall healing status of the cluster.
type HealStatus string

const (
	// HealStatusHealthy indicates all chunks are healthy.
	HealStatusHealthy HealStatus = "healthy"
	// HealStatusHealing indicates some chunks are being healed.
	HealStatusHealing HealStatus = "healing"
	// HealStatusCritical indicates too many chunks are unhealthy.
	HealStatusCritical HealStatus = "critical"
	// HealStatusUnknown indicates health status cannot be determined.
	HealStatusUnknown HealStatus = "unknown"
)

// jsonClusterHeal represents cluster-level healing status in JSON output format.
type jsonClusterHeal struct {
	Status          HealStatus `json:"status"`
	TotalNodes      int        `json:"totalNodes"`
	HealthyNodes    int        `json:"healthyNodes"`
	SuspectNodes    int        `json:"suspectNodes"`
	DownNodes       int        `json:"downNodes"`
	TotalVolumes    int        `json:"totalVolumes"`
	DegradedVolumes int        `json:"degradedVolumes"`
	FailedVolumes   int        `json:"failedVolumes"`
}

// jsonNodeHealth represents a node's health status in JSON output format.
type jsonNodeHealth struct {
	NodeID         string `json:"nodeId"`
	Address        string `json:"address"`
	Status         string `json:"status"`
	LastHeartbeat  string `json:"lastHeartbeat"`
	SecondsSince   int64  `json:"secondsSinceLastHeartbeat"`
	TotalCapacity  int64  `json:"totalCapacity"`
	AvailableBytes int64  `json:"availableCapacity"`
}

// jsonDegradedVolume represents a volume that needs healing in JSON output format.
type jsonDegradedVolume struct {
	VolumeID        string `json:"volumeId"`
	Pool            string `json:"pool"`
	MissingReplicas int    `json:"missingReplicas"`
	AffectedChunks  int    `json:"affectedChunks"`
}

// healCmd is the parent command for healing status checking.
var healCmd = &cobra.Command{
	Use:   "heal",
	Short: "Check healing and recovery status",
	Long:  "Check the status of data healing operations and node health recovery.",
}

var healStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster healing status",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()

		// Thresholds for node health classification.
		const (
			suspectTimeout  = 30 * time.Second
			downTimeout     = 60 * time.Second
			defaultReplicas = 3
		)

		now := time.Now()

		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes: %w", err)
		}

		volumes, err := client.ListVolumesMeta(ctx)
		if err != nil {
			return fmt.Errorf("listing volumes: %w", err)
		}

		placements, err := client.ListPlacementMaps(ctx)
		if err != nil {
			return fmt.Errorf("listing placement maps: %w", err)
		}

		// Classify node health.
		healthyNodes := 0
		suspectNodes := 0
		downNodes := 0
		liveNodes := make(map[string]bool)

		for _, n := range nodes {
			lastHeartbeat := time.Unix(n.LastHeartbeat, 0)
			elapsed := now.Sub(lastHeartbeat)

			if n.Status != "ready" || elapsed > downTimeout {
				downNodes++
			} else if elapsed > suspectTimeout {
				suspectNodes++
				liveNodes[n.NodeID] = true // Still accessible but suspect
			} else {
				healthyNodes++
				liveNodes[n.NodeID] = true
			}
		}

		// Build placement index.
		placementIndex := make(map[string]*metadata.PlacementMap)
		for _, pm := range placements {
			placementIndex[pm.ChunkID] = pm
		}

		// Analyze volume compliance to find degraded/failed volumes.
		degradedVolumes := 0
		failedVolumes := 0

		for _, vol := range volumes {
			volumeDegraded := false
			volumeFailed := false

			for _, chunkID := range vol.ChunkIDs {
				pm, exists := placementIndex[chunkID]
				if !exists {
					volumeFailed = true
					break
				}
				liveReplicas := 0
				for _, nodeID := range pm.Nodes {
					if liveNodes[nodeID] {
						liveReplicas++
					}
				}
				if liveReplicas == 0 {
					volumeFailed = true
					break
				}
				if liveReplicas < defaultReplicas {
					volumeDegraded = true
				}
			}

			if volumeFailed {
				failedVolumes++
			} else if volumeDegraded {
				degradedVolumes++
			}
		}

		// Determine overall cluster status.
		var status HealStatus
		switch {
		case failedVolumes > 0 || downNodes > 0:
			status = HealStatusCritical
		case degradedVolumes > 0 || suspectNodes > 0:
			status = HealStatusHealing
		default:
			status = HealStatusHealthy
		}

		if output == "json" {
			result := jsonClusterHeal{
				Status:          status,
				TotalNodes:      len(nodes),
				HealthyNodes:    healthyNodes,
				SuspectNodes:    suspectNodes,
				DownNodes:       downNodes,
				TotalVolumes:    len(volumes),
				DegradedVolumes: degradedVolumes,
				FailedVolumes:   failedVolumes,
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		}

		fmt.Println("=== NovaStor Cluster Healing Status ===")
		fmt.Printf("Overall Status:    %s\n", status)
		fmt.Println()
		fmt.Println("Nodes:")
		fmt.Printf("  Total:           %d\n", len(nodes))
		fmt.Printf("  Healthy:         %d\n", healthyNodes)
		fmt.Printf("  Suspect:         %d\n", suspectNodes)
		fmt.Printf("  Down:            %d\n", downNodes)
		fmt.Println()
		fmt.Println("Volumes:")
		fmt.Printf("  Total:           %d\n", len(volumes))
		fmt.Printf("  Degraded:        %d\n", degradedVolumes)
		fmt.Printf("  Failed:          %d\n", failedVolumes)

		return nil
	},
}

var healNodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Show node health for healing assessment",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		now := time.Now()

		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes: %w", err)
		}

		if len(nodes) == 0 {
			if output == "json" {
				return json.NewEncoder(os.Stdout).Encode([]jsonNodeHealth{})
			}
			fmt.Println("No storage nodes registered.")
			return nil
		}

		headers := []string{"NODE ID", "ADDRESS", "STATUS", "SINCE HB", "LAST HEARTBEAT"}
		var rows [][]string
		var jsonNodes []jsonNodeHealth

		for _, n := range nodes {
			lastHeartbeat := time.Unix(n.LastHeartbeat, 0)
			elapsed := now.Sub(lastHeartbeat)

			sinceStr := formatDuration(elapsed)
			hbTime := "never"
			if n.LastHeartbeat > 0 {
				hbTime = lastHeartbeat.UTC().Format("2006-01-02T15:04:05Z")
			}

			rows = append(rows, []string{
				n.NodeID,
				n.Address,
				n.Status,
				sinceStr,
				hbTime,
			})

			jsonNodes = append(jsonNodes, jsonNodeHealth{
				NodeID:         n.NodeID,
				Address:        n.Address,
				Status:         n.Status,
				LastHeartbeat:  hbTime,
				SecondsSince:   int64(elapsed.Seconds()),
				TotalCapacity:  n.TotalCapacity,
				AvailableBytes: n.AvailableCapacity,
			})
		}

		printTableOrJSON(headers, rows, jsonNodes)
		return nil
	},
}

var healVolumesCmd = &cobra.Command{
	Use:   "volumes",
	Short: "Show volumes that need healing",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		now := time.Now()

		const defaultReplicas = 3

		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes: %w", err)
		}

		liveNodes := make(map[string]bool)
		for _, n := range nodes {
			if n.Status == "ready" {
				lastHeartbeat := time.Unix(n.LastHeartbeat, 0)
				if now.Sub(lastHeartbeat) < 60*time.Second {
					liveNodes[n.NodeID] = true
				}
			}
		}

		volumes, err := client.ListVolumesMeta(ctx)
		if err != nil {
			return fmt.Errorf("listing volumes: %w", err)
		}

		placements, err := client.ListPlacementMaps(ctx)
		if err != nil {
			return fmt.Errorf("listing placement maps: %w", err)
		}

		placementIndex := make(map[string]*metadata.PlacementMap)
		for _, pm := range placements {
			placementIndex[pm.ChunkID] = pm
		}

		type volIssue struct {
			volumeID        string
			pool            string
			missingReplicas int
			affectedChunks  int
		}

		var issues []volIssue
		var jsonIssues []jsonDegradedVolume

		for _, vol := range volumes {
			totalMissing := 0
			affectedChunks := 0

			for _, chunkID := range vol.ChunkIDs {
				pm, exists := placementIndex[chunkID]
				if !exists {
					// Chunk has no placement info - treat as fully missing.
					totalMissing += defaultReplicas
					affectedChunks++
					continue
				}

				liveReplicas := 0
				for _, nodeID := range pm.Nodes {
					if liveNodes[nodeID] {
						liveReplicas++
					}
				}

				if liveReplicas < defaultReplicas {
					totalMissing += (defaultReplicas - liveReplicas)
					affectedChunks++
				}
			}

			if affectedChunks > 0 {
				issues = append(issues, volIssue{
					volumeID:        vol.VolumeID,
					pool:            vol.Pool,
					missingReplicas: totalMissing,
					affectedChunks:  affectedChunks,
				})

				jsonIssues = append(jsonIssues, jsonDegradedVolume{
					VolumeID:        vol.VolumeID,
					Pool:            vol.Pool,
					MissingReplicas: totalMissing,
					AffectedChunks:  affectedChunks,
				})
			}
		}

		if len(issues) == 0 {
			if output == "json" {
				return json.NewEncoder(os.Stdout).Encode([]jsonDegradedVolume{})
			}
			fmt.Println("All volumes are healthy. No healing needed.")
			return nil
		}

		headers := []string{"VOLUME ID", "POOL", "MISSING REPLICAS", "AFFECTED CHUNKS"}
		var rows [][]string
		for _, issue := range issues {
			rows = append(rows, []string{
				issue.volumeID,
				issue.pool,
				strconv.Itoa(issue.missingReplicas),
				strconv.Itoa(issue.affectedChunks),
			})
		}
		printTableOrJSON(headers, rows, jsonIssues)
		return nil
	},
}

// formatDuration converts a duration to a human-readable string.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return "< 1s"
	}
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

func init() {
	healCmd.AddCommand(healStatusCmd, healNodesCmd, healVolumesCmd)
	rootCmd.AddCommand(healCmd)
}
