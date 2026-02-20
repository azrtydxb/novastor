// Package main provides the NovaStor CSI driver binary.
// The CSI driver implements the Container Storage Interface for provisioning
// and managing NovaStor block volumes in Kubernetes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"

	"github.com/piwi3910/novastor/internal/agent"
	novcsi "github.com/piwi3910/novastor/internal/csi"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/placement"
	"github.com/piwi3910/novastor/internal/transport"
)

// compositeController is a CSI ControllerServer that delegates each RPC group
// to a focused sub-controller. CreateVolume/DeleteVolume/ValidateVolumeCapabilities
// go to volume, snapshot RPCs go to snapshot, and ControllerExpandVolume goes to
// expand. All three embed csi.UnimplementedControllerServer, so RPCs not covered
// by any sub-controller return an Unimplemented status automatically.
type compositeController struct {
	csi.UnimplementedControllerServer
	volume   *novcsi.ControllerServer
	snapshot *novcsi.SnapshotController
	expand   *novcsi.ExpandController
}

func (c *compositeController) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return c.volume.CreateVolume(ctx, req)
}

func (c *compositeController) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return c.volume.DeleteVolume(ctx, req)
}

func (c *compositeController) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return c.volume.ValidateVolumeCapabilities(ctx, req)
}

func (c *compositeController) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return c.volume.ListVolumes(ctx, req)
}

func (c *compositeController) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return c.volume.ControllerGetCapabilities(ctx, req)
}

func (c *compositeController) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return c.snapshot.CreateSnapshot(ctx, req)
}

func (c *compositeController) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return c.snapshot.DeleteSnapshot(ctx, req)
}

func (c *compositeController) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return c.snapshot.ListSnapshots(ctx, req)
}

func (c *compositeController) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return c.expand.ControllerExpandVolume(ctx, req)
}

// snapshotStoreAdapter bridges *metadata.GRPCClient to the novcsi.SnapshotStore
// interface. It converts between metadata.SnapshotMeta and novcsi.SnapshotMeta
// so that the CSI SnapshotController can use the remote metadata service without
// creating an import cycle between the csi and metadata packages.
type snapshotStoreAdapter struct {
	client *metadata.GRPCClient
}

func (a *snapshotStoreAdapter) PutVolumeMeta(ctx context.Context, meta *metadata.VolumeMeta) error {
	return a.client.PutVolumeMeta(ctx, meta)
}

func (a *snapshotStoreAdapter) GetVolumeMeta(ctx context.Context, volumeID string) (*metadata.VolumeMeta, error) {
	return a.client.GetVolumeMeta(ctx, volumeID)
}

func (a *snapshotStoreAdapter) DeleteVolumeMeta(ctx context.Context, volumeID string) error {
	return a.client.DeleteVolumeMeta(ctx, volumeID)
}

func (a *snapshotStoreAdapter) ListVolumesMeta(ctx context.Context) ([]*metadata.VolumeMeta, error) {
	return a.client.ListVolumesMeta(ctx)
}

func (a *snapshotStoreAdapter) PutPlacementMap(ctx context.Context, pm *metadata.PlacementMap) error {
	return a.client.PutPlacementMap(ctx, pm)
}

func (a *snapshotStoreAdapter) DeletePlacementMap(ctx context.Context, chunkID string) error {
	return a.client.DeletePlacementMap(ctx, chunkID)
}

func (a *snapshotStoreAdapter) PutSnapshotMeta(ctx context.Context, meta *novcsi.SnapshotMeta) error {
	return a.client.PutSnapshot(ctx, &metadata.SnapshotMeta{
		SnapshotID:     meta.SnapshotID,
		SourceVolumeID: meta.SourceVolumeID,
		SizeBytes:      meta.SizeBytes,
		ChunkIDs:       meta.ChunkIDs,
		CreationTime:   meta.CreationTime,
		ReadyToUse:     true,
	})
}

func (a *snapshotStoreAdapter) GetSnapshotMeta(ctx context.Context, snapshotID string) (*novcsi.SnapshotMeta, error) {
	m, err := a.client.GetSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	return &novcsi.SnapshotMeta{
		SnapshotID:     m.SnapshotID,
		SourceVolumeID: m.SourceVolumeID,
		SizeBytes:      m.SizeBytes,
		ChunkIDs:       m.ChunkIDs,
		CreationTime:   m.CreationTime,
	}, nil
}

func (a *snapshotStoreAdapter) DeleteSnapshotMeta(ctx context.Context, snapshotID string) error {
	return a.client.DeleteSnapshot(ctx, snapshotID)
}

func (a *snapshotStoreAdapter) ListSnapshotMetas(ctx context.Context) ([]*novcsi.SnapshotMeta, error) {
	all, err := a.client.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*novcsi.SnapshotMeta, 0, len(all))
	for _, m := range all {
		result = append(result, &novcsi.SnapshotMeta{
			SnapshotID:     m.SnapshotID,
			SourceVolumeID: m.SourceVolumeID,
			SizeBytes:      m.SizeBytes,
			ChunkIDs:       m.ChunkIDs,
			CreationTime:   m.CreationTime,
		})
	}
	return result, nil
}

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	endpoint := flag.String("endpoint", "unix:///var/lib/kubelet/plugins/novastor.csi.novastor.io/csi.sock", "CSI endpoint")
	nodeID := flag.String("node-id", "", "Node ID for this CSI node")
	nodeZone := flag.String("node-zone", "", "Node zone for topology (e.g., us-west-1a)")
	nodeRegion := flag.String("node-region", "", "Node region for topology (e.g., us-west-1)")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	agentAddrs := flag.String("agent-addrs", "", "Comma-separated list of agent addresses (nodeID=addr,...)")
	failureDomain := flag.String("failure-domain", "node", "CRUSH failure domain for placement: node, rack, or zone")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to client certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to client key for mTLS")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	flag.Parse()

	log.Printf("novastor-csi %s (commit: %s, built: %s)", version, commit, date)

	// Main context for the CSI driver lifetime.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build TLS dial options when TLS flags are provided.
	// We create two sets of dial options:
	//   - dialOpts: for metadata service connections (uses service DNS names)
	//   - agentDialOpts: for agent connections (uses pod IPs, so we set
	//     ServerName to match the cert's DNS SAN)
	var dialOpts []grpc.DialOption
	var agentDialOpts []grpc.DialOption
	if *tlsCA != "" && *tlsCert != "" && *tlsKey != "" {
		rotator := transport.NewCertRotator(*tlsCert, *tlsKey, *tlsRotationInterval)
		rotator.Start(ctx)
		log.Printf("TLS certificate rotation enabled (cert=%s, key=%s, interval=%s)",
			*tlsCert, *tlsKey, *tlsRotationInterval)
		tlsOpt, tlsErr := transport.NewClientTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   *tlsCert,
			KeyPath:    *tlsKey,
		}, rotator)
		if tlsErr != nil {
			log.Fatalf("Failed to configure TLS: %v", tlsErr)
		}
		dialOpts = append(dialOpts, tlsOpt)

		// Agent connections use pod IPs which won't match cert DNS SANs.
		// Override ServerName so TLS verifies against a known DNS SAN.
		agentTLSOpt, agentTLSErr := transport.NewClientTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   *tlsCert,
			KeyPath:    *tlsKey,
			ServerName: "novastor-agent",
		}, rotator)
		if agentTLSErr != nil {
			log.Fatalf("Failed to configure agent TLS: %v", agentTLSErr)
		}
		agentDialOpts = append(agentDialOpts, agentTLSOpt)
	}

	// Connect to the metadata service.
	metaClient, err := metadata.Dial(*metaAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Failed to connect to metadata service at %s: %v", *metaAddr, err)
	}
	defer func() { _ = metaClient.Close() }()

	// Build the NodeChunkClient from agent addresses.
	// Use agentDialOpts for pod IP connections when TLS is enabled.
	chunkDialOpts := agentDialOpts
	if len(chunkDialOpts) == 0 {
		chunkDialOpts = dialOpts
	}
	nodeChunkClient := agent.NewNodeChunkClient(chunkDialOpts...)
	if *agentAddrs != "" {
		for _, entry := range strings.Split(*agentAddrs, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			parts := strings.SplitN(entry, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("Invalid agent address format %q, expected nodeID=addr", entry)
			}
			nID, addr := parts[0], parts[1]
			if err := nodeChunkClient.AddNode(nID, addr); err != nil {
				log.Fatalf("Failed to connect to agent node %s at %s: %v", nID, addr, err)
			}
			log.Printf("Connected to agent node %s at %s", nID, addr)
		}
	}

	// Create CRUSH placement engine with configured failure domain.
	// Nodes are populated from metadata service during syncNodes.
	placer := placement.NewCRUSHPlacer(nil)
	placer.SetFailureDomain(*failureDomain)
	log.Printf("CRUSH placement engine initialized with failure domain: %s", *failureDomain)

	// knownNodes tracks the set of addresses currently in the placer so that
	// stale entries (dead pods) can be removed on the next sync cycle.
	knownNodes := make(map[string]struct{})
	knownNodesMu := sync.Mutex{}

	// controllerRef is set after the ControllerServer is created so that
	// syncNodes can update the address→node-name mapping for PV topology.
	var controllerRef *novcsi.ControllerServer

	// nodeTTL is how long a node entry is considered fresh. Agents heartbeat
	// every 30 seconds; entries older than 3× that are almost certainly stale
	// (dead pod, restarted pod with a new IP, etc.). TTL filtering here avoids
	// the slow per-entry TCP probes that were needed before.
	const nodeTTL = 90 * time.Second

	// gcThreshold is the age after which stale node entries are deleted from
	// the metadata store. We use 10 minutes to avoid deleting entries for
	// nodes that are temporarily unreachable but still valid.
	const gcThreshold = 10 * time.Minute

	// Sync node list from metadata service: discover ready agents dynamically.
	// This runs once at startup and every 30 seconds thereafter so that nodes
	// joining or leaving the cluster are reflected without a CSI restart.
	// On each sync we replace the full active set, removing stale addresses.
	syncNodes := func() {
		nodes, err := metaClient.ListNodeMetas(ctx)
		if err != nil {
			log.Printf("Warning: failed to list node metas: %v", err)
			return
		}
		cutoff := time.Now().Add(-nodeTTL).Unix()
		currentNodes := make(map[string]metadata.NodeMeta)
		for _, n := range nodes {
			if n.Status != "ready" || n.Address == "" {
				continue
			}
			// Skip nodes whose last heartbeat is older than nodeTTL — they are
			// stale entries left over from pods that restarted with new IPs.
			if n.LastHeartbeat < cutoff {
				log.Printf("Skipping stale storage node at %s (last heartbeat %ds ago)",
					n.Address, time.Now().Unix()-n.LastHeartbeat)
				continue
			}
			// Skip unroutable 0.0.0.0 or host-IP addresses; only accept
			// pod-network IPs that are directly reachable from the controller pod.
			host, _, splitErr := net.SplitHostPort(n.Address)
			if splitErr != nil || host == "0.0.0.0" || host == "" {
				continue
			}
			currentNodes[n.Address] = *n
		}

		knownNodesMu.Lock()
		defer knownNodesMu.Unlock()

		// Remove nodes that are no longer active.
		for addr := range knownNodes {
			if _, ok := currentNodes[addr]; !ok {
				placer.RemoveNode(addr)
				log.Printf("Removed stale storage node at %s", addr)
				delete(knownNodes, addr)
			}
		}
		// Add or update newly discovered nodes with full topology info.
		// Also build address→node-name mapping for PV topology resolution.
		addrToName := make(map[string]string, len(currentNodes))
		for addr, n := range currentNodes {
			weight := 1.0
			if n.TotalCapacity > 0 {
				// Normalize weight by capacity: 1TB = 1.0 weight unit
				weight = float64(n.TotalCapacity) / (1024 * 1024 * 1024 * 1024)
			}
			placer.AddWeightedNode(placement.Node{
				ID:     addr,
				Weight: weight,
				Zone:   n.Zone,
				Rack:   n.Rack,
			})
			if _, exists := knownNodes[addr]; !exists {
				log.Printf("Discovered storage node at %s (nodeID=%s, zone=%s, rack=%s, weight=%.2f)",
					addr, n.NodeID, n.Zone, n.Rack, weight)
				knownNodes[addr] = struct{}{}
			}
			addrToName[addr] = n.NodeID
		}

		// Update the controller's address→node-name mapping so that
		// buildVolumeTopology produces K8s node names instead of pod IPs.
		if controllerRef != nil {
			controllerRef.UpdateNodeMapping(addrToName)
		}
	}
	syncNodes()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				syncNodes()
			}
		}
	}()

	// garbageCollectNodes periodically removes stale node entries from the
	// metadata store. This prevents unbounded growth of the Raft log and
	// BadgerDB when agent pods restart with new IPs.
	garbageCollectNodes := func() {
		nodes, err := metaClient.ListNodeMetas(ctx)
		if err != nil {
			log.Printf("Warning: failed to list node metas for GC: %v", err)
			return
		}
		gcCutoff := time.Now().Add(-gcThreshold).Unix()
		deletedCount := 0
		for _, n := range nodes {
			// Delete entries that haven't had a heartbeat in gcThreshold.
			// We only delete entries that are significantly older than the
			// nodeTTL to avoid deleting nodes that are temporarily unreachable.
			if n.LastHeartbeat < gcCutoff {
				if err := metaClient.DeleteNodeMeta(ctx, n.NodeID); err != nil {
					log.Printf("Warning: failed to delete stale node %s: %v", n.NodeID, err)
				} else {
					deletedCount++
					log.Printf("Garbage collected stale node %s (last heartbeat %ds ago)",
						n.NodeID, time.Now().Unix()-n.LastHeartbeat)
				}
			}
		}
		if deletedCount > 0 {
			log.Printf("Garbage collected %d stale node entries", deletedCount)
		}
	}
	// Run GC every5 minutes.
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				garbageCollectNodes()
			}
		}
	}()

	// Parse endpoint scheme and address.
	scheme, addr, err := parseEndpoint(*endpoint)
	if err != nil {
		log.Fatalf("Invalid endpoint %q: %v", *endpoint, err)
	}

	if scheme == "unix" {
		// Remove stale socket file.
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			log.Fatalf("Failed to remove socket %s: %v", addr, err)
		}
	}

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, scheme, addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s://%s: %v", scheme, addr, err)
	}
	defer func() { _ = listener.Close() }()

	srv := grpc.NewServer()

	// Register CSI Identity service.
	identity := novcsi.NewIdentityServer()
	csi.RegisterIdentityServer(srv, identity)

	// Register CSI Node service (requires node ID).
	if *nodeID != "" {
		mounter := &novcsi.RealMounter{}
		initiator := &novcsi.LinuxInitiator{}

		// Use topology-aware constructor if zone/region provided.
		var node *novcsi.NodeService
		if *nodeZone != "" || *nodeRegion != "" {
			node = novcsi.NewNodeServiceWithInitiatorAndTopology(*nodeID, *nodeZone, *nodeRegion, nodeChunkClient, mounter, initiator)
			log.Printf("Node service initialized with topology: zone=%s, region=%s", *nodeZone, *nodeRegion)
		} else {
			node = novcsi.NewNodeServiceWithInitiator(*nodeID, nodeChunkClient, mounter, initiator)
		}
		csi.RegisterNodeServer(srv, node)
	}

	// Build NVMe target client for controller → agent communication.
	// Use agentDialOpts which has ServerName override for pod IP connections.
	targetDialOpts := agentDialOpts
	if len(targetDialOpts) == 0 {
		targetDialOpts = dialOpts
	}
	nvmeTargetClient := novcsi.NewNodeTargetClient(targetDialOpts...)

	// Build sub-controllers.
	controller := novcsi.NewControllerServer(metaClient, placer, nvmeTargetClient, nil)
	controllerRef = controller // Enable syncNodes to update the node name mapping.
	syncNodes()                // Re-run to populate the mapping now that controllerRef is set.
	snapAdapter := &snapshotStoreAdapter{client: metaClient}
	snapshotCtrl := novcsi.NewSnapshotController(snapAdapter)
	expandCtrl := novcsi.NewExpandController(metaClient, placer)

	// Register a composite ControllerServer that delegates volume, snapshot,
	// and expand RPCs to their respective sub-controllers. All three embed
	// csi.UnimplementedControllerServer so any RPC not handled by a specific
	// sub-controller falls back to the unimplemented stub automatically.
	composite := &compositeController{
		volume:   controller,
		snapshot: snapshotCtrl,
		expand:   expandCtrl,
	}
	csi.RegisterControllerServer(srv, composite)

	log.Printf("CSI driver listening on %s://%s", scheme, addr)

	// Start a lightweight HTTP health server for Kubernetes probes.
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	healthServer := &http.Server{
		Addr:         ":9808",
		Handler:      healthMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("CSI health server listening on :9808")
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGTERM/SIGINT.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		log.Println("Shutting down CSI driver...")
		cancel() // cancel main context: stops cert rotator
		srv.GracefulStop()
	}()

	if err := srv.Serve(listener); err != nil {
		log.Fatalf("CSI gRPC server failed: %v", err)
	}
}

// parseEndpoint extracts scheme and address from a CSI endpoint string.
func parseEndpoint(ep string) (string, string, error) {
	if len(ep) > 7 && ep[:7] == "unix://" {
		return "unix", ep[7:], nil
	}
	if len(ep) > 6 && ep[:6] == "tcp://" {
		return "tcp", ep[6:], nil
	}
	return "", "", fmt.Errorf("unsupported endpoint scheme in %q", ep)
}
