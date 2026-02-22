// Package filer provides file system functionality for NovaStor.
// This package implements adapters between the filer interfaces and
// the underlying metadata/chunk storage services.
package filer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/metadata"
)

// MetadataAdapter wraps a metadata.GRPCClient to satisfy the filer.MetadataClient
// interface by converting between filer-local types and metadata service types.
type MetadataAdapter struct {
	client *metadata.GRPCClient
}

// NewMetadataAdapter creates a MetadataAdapter backed by the given GRPCClient.
func NewMetadataAdapter(client *metadata.GRPCClient) *MetadataAdapter {
	return &MetadataAdapter{client: client}
}

// CreateInode stores inode metadata via the metadata service.
func (a *MetadataAdapter) CreateInode(ctx context.Context, meta *InodeMeta) error {
	return a.client.CreateInode(ctx, toMetaInode(meta))
}

// GetInode retrieves inode metadata from the metadata service.
func (a *MetadataAdapter) GetInode(ctx context.Context, ino uint64) (*InodeMeta, error) {
	mi, err := a.client.GetInode(ctx, ino)
	if err != nil {
		return nil, err
	}
	return fromMetaInode(mi), nil
}

// UpdateInode updates inode metadata via the metadata service.
func (a *MetadataAdapter) UpdateInode(ctx context.Context, meta *InodeMeta) error {
	return a.client.UpdateInode(ctx, toMetaInode(meta))
}

// DeleteInode removes inode metadata via the metadata service.
func (a *MetadataAdapter) DeleteInode(ctx context.Context, ino uint64) error {
	return a.client.DeleteInode(ctx, ino)
}

// CreateDirEntry stores a directory entry via the metadata service.
func (a *MetadataAdapter) CreateDirEntry(ctx context.Context, parentIno uint64, entry *DirEntry) error {
	return a.client.CreateDirEntry(ctx, parentIno, toMetaDirEntry(entry))
}

// DeleteDirEntry removes a directory entry via the metadata service.
func (a *MetadataAdapter) DeleteDirEntry(ctx context.Context, parentIno uint64, name string) error {
	return a.client.DeleteDirEntry(ctx, parentIno, name)
}

// LookupDirEntry retrieves a directory entry from the metadata service.
func (a *MetadataAdapter) LookupDirEntry(ctx context.Context, parentIno uint64, name string) (*DirEntry, error) {
	de, err := a.client.LookupDirEntry(ctx, parentIno, name)
	if err != nil {
		return nil, err
	}
	return fromMetaDirEntry(de), nil
}

// AllocateIno atomically allocates the next inode number via the metadata service.
func (a *MetadataAdapter) AllocateIno(ctx context.Context) (uint64, error) {
	return a.client.AllocateIno(ctx)
}

// ListDirectory returns all directory entries for the given parent inode.
func (a *MetadataAdapter) ListDirectory(ctx context.Context, parentIno uint64) ([]*DirEntry, error) {
	entries, err := a.client.ListDirectory(ctx, parentIno)
	if err != nil {
		return nil, err
	}
	result := make([]*DirEntry, len(entries))
	for i, de := range entries {
		result[i] = fromMetaDirEntry(de)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

func toMetaInode(fi *InodeMeta) *metadata.InodeMeta {
	return &metadata.InodeMeta{
		Ino:       fi.Ino,
		Type:      metadata.InodeType(fi.Type),
		Size:      fi.Size,
		Mode:      fi.Mode,
		UID:       fi.UID,
		GID:       fi.GID,
		LinkCount: fi.LinkCount,
		ChunkIDs:  fi.ChunkIDs,
		Target:    fi.Target,
		Xattrs:    fi.Xattrs,
		ATime:     fi.ATime,
		MTime:     fi.MTime,
		CTime:     fi.CTime,
	}
}

func fromMetaInode(mi *metadata.InodeMeta) *InodeMeta {
	return &InodeMeta{
		Ino:       mi.Ino,
		Type:      InodeType(mi.Type),
		Size:      mi.Size,
		Mode:      mi.Mode,
		UID:       mi.UID,
		GID:       mi.GID,
		LinkCount: mi.LinkCount,
		ChunkIDs:  mi.ChunkIDs,
		Target:    mi.Target,
		Xattrs:    mi.Xattrs,
		ATime:     mi.ATime,
		MTime:     mi.MTime,
		CTime:     mi.CTime,
	}
}

func toMetaDirEntry(de *DirEntry) *metadata.DirEntry {
	return &metadata.DirEntry{
		Name: de.Name,
		Ino:  de.Ino,
		Type: metadata.InodeType(de.Type),
	}
}

func fromMetaDirEntry(de *metadata.DirEntry) *DirEntry {
	return &DirEntry{
		Name: de.Name,
		Ino:  de.Ino,
		Type: InodeType(de.Type),
	}
}

// ---------------------------------------------------------------------------
// ChunkAdapter — adapts agent.Client to satisfy filer.ChunkClient
// ---------------------------------------------------------------------------

// chunkDataSize is the size of each data chunk for writing (4 MiB).
const chunkDataSize = 4 * 1024 * 1024

// ChunkAdapter wraps an agent.Client to satisfy the filer.ChunkClient interface.
type ChunkAdapter struct {
	client *agent.Client
}

// NewChunkAdapter creates a ChunkAdapter backed by the given agent Client.
func NewChunkAdapter(client *agent.Client) *ChunkAdapter {
	return &ChunkAdapter{client: client}
}

// ReadChunks reads data from the given chunk IDs, applying offset and length.
func (a *ChunkAdapter) ReadChunks(ctx context.Context, chunkIDs []string, offset, length int64) ([]byte, error) {
	var allData []byte
	for _, id := range chunkIDs {
		data, _, err := a.client.GetChunk(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("reading chunk %s: %w", id, err)
		}
		allData = append(allData, data...)
	}

	// Apply offset and length to the concatenated data.
	if offset >= int64(len(allData)) {
		return nil, nil
	}
	end := offset + length
	if end > int64(len(allData)) {
		end = int64(len(allData))
	}
	return allData[offset:end], nil
}

// WriteChunks splits data into fixed-size chunks, writes each to the agent,
// and returns the generated chunk IDs.
func (a *ChunkAdapter) WriteChunks(ctx context.Context, data []byte) ([]string, error) {
	var chunkIDs []string
	for off := 0; off < len(data); off += chunkDataSize {
		end := off + chunkDataSize
		if end > len(data) {
			end = len(data)
		}
		segment := data[off:end]

		h := sha256.Sum256(segment)
		chunkID := hex.EncodeToString(h[:])
		checksum := crc32.Checksum(segment, crc32.MakeTable(crc32.Castagnoli))

		if err := a.client.PutChunk(ctx, chunkID, segment, checksum); err != nil {
			return nil, fmt.Errorf("writing chunk: %w", err)
		}
		chunkIDs = append(chunkIDs, chunkID)
	}

	// Handle empty data: write a single empty chunk.
	if len(data) == 0 {
		h := sha256.Sum256(nil)
		chunkID := hex.EncodeToString(h[:])
		checksum := crc32.Checksum(nil, crc32.MakeTable(crc32.Castagnoli))
		if err := a.client.PutChunk(ctx, chunkID, nil, checksum); err != nil {
			return nil, fmt.Errorf("writing empty chunk: %w", err)
		}
		chunkIDs = append(chunkIDs, chunkID)
	}

	return chunkIDs, nil
}
