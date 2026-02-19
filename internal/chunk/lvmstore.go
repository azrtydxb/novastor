// Package chunk provides LVM-based storage backend for chunks.
package chunk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/piwi3910/novastor/internal/metrics"
)

const (
	// DefaultVolumeGroup is the default LVM volume group name.
	DefaultVolumeGroup = "novastor"
	// DefaultThinPool is the default LVM thin pool name.
	DefaultThinPool = "chunks"
	// ChunkDevicePrefix is the prefix for chunk logical volumes.
	ChunkDevicePrefix = "chunk-"
)

// LVMStore implements chunk.Store using LVM thin volumes.
// Each chunk is stored as a separate thin logical volume, enabling
// efficient snapshot-based copies and thin provisioning.
type LVMStore struct {
	vgName    string
	thinPool  string
	deviceDir string
	mapperDir string
}

// NewLVMStore creates a new LVM-backed chunk store.
//
// The vgName parameter specifies the LVM volume group to use.
// The thinPool parameter specifies the thin pool within the VG.
//
// Returns an error if the volume group or thin pool does not exist.
func NewLVMStore(vgName, thinPool string) (*LVMStore, error) {
	if vgName == "" {
		vgName = DefaultVolumeGroup
	}
	if thinPool == "" {
		thinPool = DefaultThinPool
	}

	// Verify volume group exists.
	if err := lvmVGExists(vgName); err != nil {
		return nil, fmt.Errorf("validating volume group %s: %w", vgName, err)
	}

	// Verify thin pool exists.
	if err := lvmThinPoolExists(vgName, thinPool); err != nil {
		return nil, fmt.Errorf("validating thin pool %s/%s: %w", vgName, thinPool, err)
	}

	return &LVMStore{
		vgName:    vgName,
		thinPool:  thinPool,
		deviceDir: "/dev/" + vgName,
		mapperDir: "/dev/mapper/" + vgName + "-" + thinPool,
	}, nil
}

// Put stores a chunk as a thin logical volume.
func (s *LVMStore) Put(_ context.Context, c *Chunk) error {
	lvName := s.lvNameForChunk(c.ID)
	lvPath := s.lvPath(lvName)

	// Check if LV already exists.
	exists, err := lvmLVExists(s.vgName, lvName)
	if err != nil {
		return fmt.Errorf("checking if LV exists: %w", err)
	}
	if exists {
		return nil
	}

	// Create thin volume (4MB = 8192 sectors of 512 bytes).
	const chunkSectors = ChunkSize / 512
	if err := lvmCreateThinLV(s.vgName, s.thinPool, lvName, chunkSectors); err != nil {
		return fmt.Errorf("creating thin LV: %w", err)
	}

	// Write chunk data with checksum prefix.
	buf := make([]byte, 4+len(c.Data))
	binary.BigEndian.PutUint32(buf[:4], c.Checksum)
	copy(buf[4:], c.Data)

	// Write data to the device.
	if err := os.WriteFile(lvPath, buf, 0o600); err != nil {
		// Clean up the LV on write failure.
		_ = lvmRemoveLV(s.vgName, lvName)
		return fmt.Errorf("writing chunk %s: %w", c.ID, err)
	}

	metrics.ChunkOpsTotal.WithLabelValues("write").Inc()
	metrics.ChunkBytesTotal.WithLabelValues("write").Add(float64(len(c.Data)))
	return nil
}

// Get retrieves a chunk from a thin logical volume.
func (s *LVMStore) Get(_ context.Context, id ChunkID) (*Chunk, error) {
	lvName := s.lvNameForChunk(id)
	lvPath := s.lvPath(lvName)

	// Check if LV exists.
	exists, err := lvmLVExists(s.vgName, lvName)
	if err != nil {
		return nil, fmt.Errorf("checking if LV exists: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("chunk %s not found", id)
	}

	// Read data from the device.
	raw, err := os.ReadFile(lvPath)
	if err != nil {
		return nil, fmt.Errorf("reading chunk %s: %w", id, err)
	}

	if len(raw) < 4 {
		return nil, fmt.Errorf("chunk %s: data too small", id)
	}

	checksum := binary.BigEndian.Uint32(raw[:4])
	data := raw[4:]

	c := &Chunk{ID: id, Data: data, Checksum: checksum}
	if err := c.VerifyChecksum(); err != nil {
		return nil, fmt.Errorf("chunk %s integrity check failed: %w", id, err)
	}

	metrics.ChunkOpsTotal.WithLabelValues("read").Inc()
	metrics.ChunkBytesTotal.WithLabelValues("read").Add(float64(len(data)))
	return c, nil
}

// Delete removes a chunk's thin logical volume.
func (s *LVMStore) Delete(_ context.Context, id ChunkID) error {
	lvName := s.lvNameForChunk(id)

	exists, err := lvmLVExists(s.vgName, lvName)
	if err != nil {
		return fmt.Errorf("checking if LV exists: %w", err)
	}
	if !exists {
		return nil
	}

	if err := lvmRemoveLV(s.vgName, lvName); err != nil {
		return fmt.Errorf("deleting chunk %s: %w", id, err)
	}

	metrics.ChunkOpsTotal.WithLabelValues("delete").Inc()
	return nil
}

// Has checks if a chunk's logical volume exists.
func (s *LVMStore) Has(_ context.Context, id ChunkID) (bool, error) {
	lvName := s.lvNameForChunk(id)
	return lvmLVExists(s.vgName, lvName)
}

// List returns all chunk IDs by listing logical volumes with the chunk prefix.
func (s *LVMStore) List(_ context.Context) ([]ChunkID, error) {
	lvs, err := lvmListLVs(s.vgName, ChunkDevicePrefix)
	if err != nil {
		return nil, fmt.Errorf("listing LVs: %w", err)
	}

	ids := make([]ChunkID, 0, len(lvs))
	for _, lv := range lvs {
		// Extract chunk ID from LV name: "chunk-<id>" -> "<id>"
		id := strings.TrimPrefix(lv, ChunkDevicePrefix)
		if id != lv {
			ids = append(ids, ChunkID(id))
		}
	}

	return ids, nil
}

// lvNameForChunk converts a chunk ID to an LVM-safe LV name.
func (s *LVMStore) lvNameForChunk(id ChunkID) string {
	// LVM names must be safe filesystem names.
	// Chunk IDs are hex-encoded SHA256, so they're already safe.
	// Prefix with "chunk-" to identify chunk volumes.
	return ChunkDevicePrefix + string(id)
}

// lvPath returns the device path for a logical volume.
func (s *LVMStore) lvPath(lvName string) string {
	// Use /dev/<vg>/<lv> format which is always available.
	return filepath.Join("/dev", s.vgName, lvName)
}

// Snapshot creates a snapshot of a chunk's logical volume.
// The snapshot is a new thin volume that shares data with the source
// until written to (copy-on-write).
func (s *LVMStore) Snapshot(_ context.Context, sourceID, snapshotID ChunkID) error {
	sourceLV := s.lvNameForChunk(sourceID)
	snapLV := s.lvNameForChunk(snapshotID)

	// Verify source exists.
	exists, err := lvmLVExists(s.vgName, sourceLV)
	if err != nil {
		return fmt.Errorf("checking if source LV exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("source chunk %s not found", sourceID)
	}

	// Check if snapshot already exists.
	exists, err = lvmLVExists(s.vgName, snapLV)
	if err != nil {
		return fmt.Errorf("checking if snapshot LV exists: %w", err)
	}
	if exists {
		return nil
	}

	// Create snapshot (thin snapshot in the same pool).
	if err := lvmCreateSnapshot(s.vgName, s.thinPool, sourceLV, snapLV); err != nil {
		return fmt.Errorf("creating snapshot: %w", err)
	}

	return nil
}

// Resize changes the size of a chunk's logical volume.
// This is primarily used for testing or special cases where
// chunk sizes differ from the default.
func (s *LVMStore) Resize(_ context.Context, id ChunkID, newSizeBytes int64) error {
	lvName := s.lvNameForChunk(id)

	exists, err := lvmLVExists(s.vgName, lvName)
	if err != nil {
		return fmt.Errorf("checking if LV exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("chunk %s not found", id)
	}

	// LVM works in 512-byte sectors.
	newSectors := newSizeBytes / 512
	if err := lvmResizeLV(s.vgName, lvName, newSectors); err != nil {
		return fmt.Errorf("resizing LV: %w", err)
	}

	return nil
}

// Capacity returns the capacity information for the thin pool.
func (s *LVMStore) Capacity(_ context.Context) (totalBytes, freeBytes int64, err error) {
	return lvmThinPoolCapacity(s.vgName, s.thinPool)
}

// ---- LVM command helpers ----

// lvmVGExists checks if a volume group exists.
func lvmVGExists(vgName string) error {
	out, err := exec.CommandContext(context.Background(), "vgs", "--noheadings", "-o", "vg_name", vgName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("vgs command failed: %w, output: %s", err, string(out))
	}
	if len(bytes.TrimSpace(out)) == 0 {
		return fmt.Errorf("volume group %q not found", vgName)
	}
	return nil
}

// lvmThinPoolExists checks if a thin pool exists in a volume group.
func lvmThinPoolExists(vgName, poolName string) error {
	out, err := exec.CommandContext(context.Background(), "lvs", "--noheadings", "-o", "lv_name", vgName+"/"+poolName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("lvs command failed: %w, output: %s", err, string(out))
	}
	if len(bytes.TrimSpace(out)) == 0 {
		return fmt.Errorf("thin pool %q not found in VG %q", poolName, vgName)
	}
	return nil
}

// lvmLVExists checks if a logical volume exists.
func lvmLVExists(vgName, lvName string) (bool, error) {
	out, err := exec.CommandContext(context.Background(), "lvs", "--noheadings", "-o", "lv_name", vgName+"/"+lvName).CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "Failed to find logical volume") {
			return false, nil
		}
		return false, fmt.Errorf("lvs command failed: %w, output: %s", err, string(out))
	}
	return len(bytes.TrimSpace(out)) > 0, nil
}

// lvmCreateThinLV creates a new thin logical volume.
// sizeSectors is the size in 512-byte sectors.
func lvmCreateThinLV(vgName, poolName, lvName string, sizeSectors int64) error {
	// lvcreate -V <size> --thin --name <lv> <vg>/<pool>
	args := []string{
		"-V", strconv.FormatInt(sizeSectors*512, 10) + "B",
		"--thin",
		"--name", lvName,
		vgName + "/" + poolName,
	}
	out, err := exec.CommandContext(context.Background(), "lvcreate", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("lvcreate failed: %w, output: %s", err, string(out))
	}
	return nil
}

// lvmRemoveLV removes a logical volume.
func lvmRemoveLV(vgName, lvName string) error {
	// lvremove -f <vg>/<lv>
	out, err := exec.CommandContext(context.Background(), "lvremove", "-f", vgName+"/"+lvName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("lvremove failed: %w, output: %s", err, string(out))
	}
	return nil
}

// lvmListLVs lists logical volumes in a volume group with a given prefix.
func lvmListLVs(vgName, prefix string) ([]string, error) {
	// lvs --noheadings -o lv_name <vg>
	// Filter for volumes starting with prefix.
	out, err := exec.CommandContext(context.Background(), "lvs", "--noheadings", "-o", "lv_name", vgName).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("lvs command failed: %w, output: %s", err, string(out))
	}

	lines := strings.Split(string(out), "\n")
	var lvs []string
	for _, line := range lines {
		lv := strings.TrimSpace(line)
		if lv != "" && strings.HasPrefix(lv, prefix) {
			lvs = append(lvs, lv)
		}
	}
	return lvs, nil
}

// lvmCreateSnapshot creates a snapshot of a logical volume.
func lvmCreateSnapshot(vgName, _ /* poolName */, sourceLV, snapLV string) error {
	// lvcreate -s --name <snap> <vg>/<source>
	args := []string{
		"-s",
		"--name", snapLV,
		vgName + "/" + sourceLV,
	}
	out, err := exec.CommandContext(context.Background(), "lvcreate", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("lvcreate snapshot failed: %w, output: %s", err, string(out))
	}
	return nil
}

// lvmResizeLV resizes a logical volume.
func lvmResizeLV(vgName, lvName string, newSectors int64) error {
	// lvresize -f -L <size> <vg>/<lv>
	args := []string{
		"-f",
		"-L", strconv.FormatInt(newSectors*512, 10) + "B",
		vgName + "/" + lvName,
	}
	out, err := exec.CommandContext(context.Background(), "lvresize", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("lvresize failed: %w, output: %s", err, string(out))
	}
	return nil
}

// lvmThinPoolCapacity returns total and free bytes for a thin pool.
func lvmThinPoolCapacity(vgName, poolName string) (totalBytes, freeBytes int64, err error) {
	// lvs --noheadings --units=b --nosuffix -o lv_size,pool_lv <vg>/<pool>
	// For thin pools, we need to get the data percent and size.
	out, err := exec.CommandContext(context.Background(), "lvs", "--noheadings", "--units=b", "--nosuffix",
		"-o", "lv_size,data_percent", vgName+"/"+poolName).CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf("lvs command failed: %w, output: %s", err, string(out))
	}

	fields := strings.Fields(string(out))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("unexpected lvs output: %s", string(out))
	}

	totalBytes, err = strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing size: %w", err)
	}

	dataPercent, err := strconv.ParseFloat(strings.TrimSuffix(fields[1], "%"), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing data percent: %w", err)
	}

	usedBytes := int64(float64(totalBytes) * dataPercent / 100)
	freeBytes = totalBytes - usedBytes

	return totalBytes, freeBytes, nil
}
