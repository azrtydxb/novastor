package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/spf13/cobra"
)

// ComplianceStatus represents the health state of a volume's data protection.
type ComplianceStatus string

const (
	// ComplianceOK indicates the volume is fully compliant with its protection policy.
	ComplianceOK ComplianceStatus = "OK"
	// ComplianceDegraded indicates the volume has some failed chunks but data is still recoverable.
	ComplianceDegraded ComplianceStatus = "DEGRADED"
	// ComplianceFailed indicates the volume has lost too much data to recover.
	ComplianceFailed ComplianceStatus = "FAILED"
)

// jsonVolumeCompliance represents a volume's compliance status in JSON output format.
type jsonVolumeCompliance struct {
	VolumeID        string           `json:"volumeId"`
	Pool            string           `json:"pool"`
	ProtectionMode  string           `json:"protectionMode"`
	Status          ComplianceStatus `json:"status"`
	TotalChunks     int              `json:"totalChunks"`
	CompliantChunks int              `json:"compliantChunks"`
	FailedChunks    int              `json:"failedChunks"`
}

// jsonChunkCompliance represents a single chunk's compliance status in JSON output format.
type jsonChunkCompliance struct {
	ChunkID          string           `json:"chunkId"`
	Status           ComplianceStatus `json:"status"`
	ReplicasExpected int              `json:"replicasExpected"`
	ReplicasActual   int              `json:"replicasActual"`
	ReplicaNodes     []string         `json:"replicaNodes,omitempty"`
}

// complianceCmd is the parent command for compliance checking.
var complianceCmd = &cobra.Command{
	Use:   "compliance",
	Short: "Check data protection compliance status",
	Long:  "Check the compliance status of volumes and their chunks against configured data protection policies.",
}

var complianceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List compliance status for all volumes",
	RunE: func(_ *cobra.Command, _ []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()

		// Fetch all volumes and nodes to compute compliance.
		volumes, err := client.ListVolumesMeta(ctx)
		if err != nil {
			return fmt.Errorf("listing volumes: %w", err)
		}

		placements, err := client.ListPlacementMaps(ctx)
		if err != nil {
			return fmt.Errorf("listing placement maps: %w", err)
		}

		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes: %w", err)
		}

		// Build a set of live node IDs.
		liveNodes := make(map[string]bool)
		for _, n := range nodes {
			if n.Status == "ready" {
				liveNodes[n.NodeID] = true
			}
		}

		// Index placement maps by chunk ID.
		placementIndex := make(map[string]*metadata.PlacementMap)
		for _, pm := range placements {
			placementIndex[pm.ChunkID] = pm
		}

		// Default replication factor for compliance checking.
		// In a full implementation, this would come from StoragePool CRD.
		defaultReplicas := 3

		type volCompliance struct {
			volumeID        string
			pool            string
			status          ComplianceStatus
			totalChunks     int
			compliantChunks int
			failedChunks    int
		}

		var volResults []volCompliance
		var jsonVolCompliances []jsonVolumeCompliance

		for _, vol := range volumes {
			total := len(vol.ChunkIDs)
			compliant := 0
			failed := 0
			status := ComplianceOK

			for _, chunkID := range vol.ChunkIDs {
				pm, exists := placementIndex[chunkID]
				if !exists {
					failed++
					continue
				}
				// Count how many replicas are on live nodes.
				liveReplicas := 0
				for _, nodeID := range pm.Nodes {
					if liveNodes[nodeID] {
						liveReplicas++
					}
				}
				if liveReplicas >= defaultReplicas {
					compliant++
				} else if liveReplicas > 0 {
					// Degraded: at least one replica but not enough.
					status = ComplianceDegraded
					failed++
				} else {
					// Failed: no live replicas.
					status = ComplianceFailed
					failed++
				}
			}

			if total == 0 {
				status = ComplianceOK // Empty volume is trivially compliant.
			}

			// Determine overall status based on chunk health.
			if failed > 0 && compliant == 0 {
				status = ComplianceFailed
			} else if failed > 0 {
				status = ComplianceDegraded
			}

			volResults = append(volResults, volCompliance{
				volumeID:        vol.VolumeID,
				pool:            vol.Pool,
				status:          status,
				totalChunks:     total,
				compliantChunks: compliant,
				failedChunks:    failed,
			})

			jsonVolCompliances = append(jsonVolCompliances, jsonVolumeCompliance{
				VolumeID:        vol.VolumeID,
				Pool:            vol.Pool,
				ProtectionMode:  "replication", // Would come from StoragePool in production.
				Status:          status,
				TotalChunks:     total,
				CompliantChunks: compliant,
				FailedChunks:    failed,
			})
		}

		if len(volResults) == 0 {
			if output == "json" {
				return json.NewEncoder(os.Stdout).Encode([]jsonVolumeCompliance{})
			}
			fmt.Println("No volumes found.")
			return nil
		}

		headers := []string{"VOLUME ID", "POOL", "STATUS", "CHUNKS", "COMPLIANT", "FAILED"}
		var rows [][]string
		for _, vr := range volResults {
			rows = append(rows, []string{
				vr.volumeID,
				vr.pool,
				string(vr.status),
				strconv.Itoa(vr.totalChunks),
				strconv.Itoa(vr.compliantChunks),
				strconv.Itoa(vr.failedChunks),
			})
		}
		printTableOrJSON(headers, rows, jsonVolCompliances)
		return nil
	},
}

var complianceGetCmd = &cobra.Command{
	Use:   "get [volume-id]",
	Short: "Get detailed compliance status for a specific volume",
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

		nodes, err := client.ListNodeMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing nodes: %w", err)
		}

		liveNodes := make(map[string]bool)
		for _, n := range nodes {
			if n.Status == "ready" {
				liveNodes[n.NodeID] = true
			}
		}

		placements, err := client.ListPlacementMaps(ctx)
		if err != nil {
			return fmt.Errorf("listing placement maps: %w", err)
		}

		placementIndex := make(map[string]*metadata.PlacementMap)
		for _, pm := range placements {
			placementIndex[pm.ChunkID] = pm
		}

		// Default replication factor.
		defaultReplicas := 3

		fmt.Printf("Volume ID:      %s\n", vol.VolumeID)
		fmt.Printf("Pool:           %s\n", vol.Pool)
		fmt.Printf("Total Chunks:   %d\n", len(vol.ChunkIDs))
		fmt.Printf("Protection:     replication (factor=%d)\n", defaultReplicas)
		fmt.Println()

		headers := []string{"CHUNK ID", "STATUS", "REPLICAS", "NODES"}
		var rows [][]string
		var jsonChunks []jsonChunkCompliance

		for _, chunkID := range vol.ChunkIDs {
			pm, exists := placementIndex[chunkID]
			var status ComplianceStatus
			var replicasActual int
			var nodeStr string

			if !exists {
				status = ComplianceFailed
				replicasActual = 0
				nodeStr = "unknown"
			} else {
				liveReplicas := 0
				var liveNodeList []string
				for _, nodeID := range pm.Nodes {
					if liveNodes[nodeID] {
						liveReplicas++
						liveNodeList = append(liveNodeList, nodeID)
					}
				}
				replicasActual = liveReplicas
				nodeStr = fmt.Sprintf("%v", liveNodeList)

				if liveReplicas >= defaultReplicas {
					status = ComplianceOK
				} else if liveReplicas > 0 {
					status = ComplianceDegraded
				} else {
					status = ComplianceFailed
				}
			}

			rows = append(rows, []string{
				chunkID,
				string(status),
				fmt.Sprintf("%d/%d", replicasActual, defaultReplicas),
				nodeStr,
			})

			jsonChunks = append(jsonChunks, jsonChunkCompliance{
				ChunkID:          chunkID,
				Status:           status,
				ReplicasExpected: defaultReplicas,
				ReplicasActual:   replicasActual,
			})
		}

		if output == "json" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(jsonChunks)
		}

		printTable(headers, rows)
		return nil
	},
}

func init() {
	complianceCmd.AddCommand(complianceListCmd, complianceGetCmd)
	rootCmd.AddCommand(complianceCmd)
}
