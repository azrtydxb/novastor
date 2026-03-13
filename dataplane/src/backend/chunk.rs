//! Chunk backend — content-addressed 4MB immutable chunk storage.
//!
//! Volumes are virtual: a volume is an ordered mapping of offsets to chunk IDs.
//! Chunks are immutable and content-addressed (SHA-256), enabling zero-cost
//! deduplication, instant snapshots (copy the index), and instant clones.
//!
//! Offset-based reads translate to: which chunk covers this offset, read that
//! chunk, return the relevant slice. Writes: split data into chunks, store
//! each, update the offset→chunk mapping.

use crate::bdev::chunk_io::{ChunkID, ChunkStore, CHUNK_SIZE};
use crate::error::{DataPlaneError, Result};
use log::info;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::traits::*;

/// A chunk-backed volume: an ordered mapping from chunk-aligned offsets to
/// chunk IDs. Gaps (never-written regions) map to `None` and read as zeros.
struct ChunkVolume {
    name: String,
    size_bytes: u64,
    /// Ordered map: chunk_index → chunk_id.
    /// chunk_index = offset / CHUNK_SIZE.
    chunks: RwLock<HashMap<u64, ChunkID>>,
    is_snapshot: bool,
    parent_snapshot: Option<String>,
}

struct ChunkSnapshot {
    name: String,
    source_volume: String,
    /// Frozen copy of the chunk index at snapshot time.
    chunks: HashMap<u64, ChunkID>,
    size_bytes: u64,
    created_at: u64,
}

pub struct ChunkBackend {
    /// The underlying chunk store for actual chunk I/O.
    store: Arc<ChunkStore>,
    volumes: Mutex<HashMap<String, ChunkVolume>>,
    snapshots: Mutex<HashMap<String, ChunkSnapshot>>,
}

impl ChunkBackend {
    /// Create a new chunk backend backed by the given ChunkStore.
    pub fn new(store: Arc<ChunkStore>) -> Self {
        info!("chunk backend initialised on store '{}'", store.bdev_name());
        Self {
            store,
            volumes: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// How many chunk slots a volume of this size needs.
    fn num_chunk_slots(size_bytes: u64) -> u64 {
        (size_bytes + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64
    }

    /// Calculate used bytes from chunk count.
    fn used_bytes(chunks: &HashMap<u64, ChunkID>) -> u64 {
        chunks.len() as u64 * CHUNK_SIZE as u64
    }
}

impl StorageBackend for ChunkBackend {
    fn backend_type(&self) -> BackendType {
        BackendType::Chunk
    }

    fn create_volume(&self, name: &str, size_bytes: u64, _thin: bool) -> Result<VolumeInfo> {
        info!("chunk: creating volume '{}' ({}B)", name, size_bytes);

        let mut volumes = self.volumes.lock().unwrap();
        if volumes.contains_key(name) {
            return Err(DataPlaneError::BdevError(format!(
                "volume '{}' already exists",
                name
            )));
        }

        let vol = ChunkVolume {
            name: name.to_string(),
            size_bytes,
            chunks: RwLock::new(HashMap::new()),
            is_snapshot: false,
            parent_snapshot: None,
        };
        volumes.insert(name.to_string(), vol);

        Ok(VolumeInfo {
            name: name.to_string(),
            backend: BackendType::Chunk,
            size_bytes,
            used_bytes: 0, // no chunks allocated yet (always thin)
            block_size: CHUNK_SIZE as u32,
            healthy: true,
            is_snapshot: false,
            parent_snapshot: None,
            thin_provisioned: true, // chunk backend is inherently thin
        })
    }

    fn delete_volume(&self, name: &str) -> Result<()> {
        info!("chunk: deleting volume '{}'", name);
        let vol = self
            .volumes
            .lock()
            .unwrap()
            .remove(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;

        // Delete chunks that are not referenced by any other volume or snapshot.
        let chunk_ids: Vec<ChunkID> = vol.chunks.read().unwrap().values().cloned().collect();
        for chunk_id in &chunk_ids {
            if !self.is_chunk_referenced_elsewhere(chunk_id, name) {
                let _ = self.store.delete_chunk(chunk_id);
            }
        }
        Ok(())
    }

    fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<VolumeInfo> {
        info!("chunk: resizing volume '{}' to {}B", name, new_size_bytes);
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;

        if new_size_bytes < vol.size_bytes {
            // Shrink: remove chunks beyond new size.
            let new_slots = Self::num_chunk_slots(new_size_bytes);
            let mut chunks = vol.chunks.write().unwrap();
            let to_remove: Vec<u64> = chunks
                .keys()
                .filter(|&&k| k >= new_slots)
                .cloned()
                .collect();
            for k in &to_remove {
                if let Some(chunk_id) = chunks.remove(k) {
                    if !self.is_chunk_referenced_elsewhere(&chunk_id, name) {
                        let _ = self.store.delete_chunk(&chunk_id);
                    }
                }
            }
        }
        // For grow: nothing to do, new chunk slots are implicitly None (zeros).

        // We can't mutate size_bytes through the shared ref, so we drop and re-acquire.
        drop(volumes);
        // Update size. We need mutable access.
        // Since ChunkVolume stores size_bytes as a plain u64, we reconstruct.
        // This is a limitation of the current design; ideally size_bytes would be atomic.
        let mut volumes = self.volumes.lock().unwrap();
        if let Some(vol) = volumes.get_mut(name) {
            // Safe: we hold the lock.
            let used = Self::used_bytes(&vol.chunks.read().unwrap());
            let old_parent = vol.parent_snapshot.clone();
            // Reconstruct with new size.
            let new_vol = ChunkVolume {
                name: name.to_string(),
                size_bytes: new_size_bytes,
                chunks: RwLock::new(vol.chunks.read().unwrap().clone()),
                is_snapshot: vol.is_snapshot,
                parent_snapshot: old_parent.clone(),
            };
            *vol = new_vol;

            return Ok(VolumeInfo {
                name: name.to_string(),
                backend: BackendType::Chunk,
                size_bytes: new_size_bytes,
                used_bytes: used,
                block_size: CHUNK_SIZE as u32,
                healthy: true,
                is_snapshot: false,
                parent_snapshot: old_parent,
                thin_provisioned: true,
            });
        }
        Err(DataPlaneError::BdevError(
            "volume disappeared during resize".into(),
        ))
    }

    fn stat_volume(&self, name: &str) -> Result<VolumeInfo> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;
        let used = Self::used_bytes(&vol.chunks.read().unwrap());
        Ok(VolumeInfo {
            name: vol.name.clone(),
            backend: BackendType::Chunk,
            size_bytes: vol.size_bytes,
            used_bytes: used,
            block_size: CHUNK_SIZE as u32,
            healthy: true,
            is_snapshot: vol.is_snapshot,
            parent_snapshot: vol.parent_snapshot.clone(),
            thin_provisioned: true,
        })
    }

    fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let volumes = self.volumes.lock().unwrap();
        Ok(volumes
            .values()
            .map(|v| {
                let used = Self::used_bytes(&v.chunks.read().unwrap());
                VolumeInfo {
                    name: v.name.clone(),
                    backend: BackendType::Chunk,
                    size_bytes: v.size_bytes,
                    used_bytes: used,
                    block_size: CHUNK_SIZE as u32,
                    healthy: true,
                    is_snapshot: v.is_snapshot,
                    parent_snapshot: v.parent_snapshot.clone(),
                    thin_provisioned: true,
                }
            })
            .collect())
    }

    fn read(&self, name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;
        if offset + length > vol.size_bytes {
            return Err(DataPlaneError::BdevError(format!(
                "read past end: offset={} length={} size={}",
                offset, length, vol.size_bytes
            )));
        }

        let chunks = vol.chunks.read().unwrap();
        let mut result = Vec::with_capacity(length as usize);
        let mut pos = offset;
        let end = offset + length;

        while pos < end {
            let chunk_idx = pos / CHUNK_SIZE as u64;
            let offset_in_chunk = (pos % CHUNK_SIZE as u64) as usize;
            let remaining_in_chunk = CHUNK_SIZE - offset_in_chunk;
            let to_read = std::cmp::min(remaining_in_chunk as u64, end - pos) as usize;

            if let Some(chunk_id) = chunks.get(&chunk_idx).cloned() {
                // Read the chunk from the store.
                drop(chunks);
                drop(volumes);
                let chunk_data = self.store.read_chunk(&chunk_id)?;
                let slice_end = std::cmp::min(offset_in_chunk + to_read, chunk_data.len());
                result.extend_from_slice(&chunk_data[offset_in_chunk..slice_end]);
                // Pad with zeros if chunk is smaller than expected.
                if slice_end - offset_in_chunk < to_read {
                    result.resize(result.len() + (to_read - (slice_end - offset_in_chunk)), 0);
                }
                // Re-acquire locks for next iteration.
                let volumes_guard = self.volumes.lock().unwrap();
                let vol_ref = volumes_guard.get(name).ok_or_else(|| {
                    DataPlaneError::BdevError("volume disappeared during read".into())
                })?;
                let chunks_guard = vol_ref.chunks.read().unwrap();
                // We need to continue the loop with these guards.
                // This is awkward — restructure to avoid holding locks across store I/O.
                pos += to_read as u64;
                // We'll break out and re-enter the pattern. For simplicity,
                // drop and re-acquire each iteration.
                drop(chunks_guard);
                drop(volumes_guard);
                if pos < end {
                    let volumes_guard = self.volumes.lock().unwrap();
                    let vol_ref = volumes_guard.get(name).ok_or_else(|| {
                        DataPlaneError::BdevError("volume disappeared during read".into())
                    })?;
                    // Continue with a recursive-style approach instead.
                    let remaining = self.read_inner(vol_ref, pos, end - pos)?;
                    result.extend_from_slice(&remaining);
                    return Ok(result);
                }
                return Ok(result);
            } else {
                // Unwritten region — return zeros.
                result.resize(result.len() + to_read, 0);
                pos += to_read as u64;
            }
        }
        Ok(result)
    }

    fn write(&self, name: &str, offset: u64, data: &[u8]) -> Result<()> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;
        if vol.is_snapshot {
            return Err(DataPlaneError::BdevError("cannot write to snapshot".into()));
        }
        if offset + data.len() as u64 > vol.size_bytes {
            return Err(DataPlaneError::BdevError(format!(
                "write past end: offset={} length={} size={}",
                offset,
                data.len(),
                vol.size_bytes
            )));
        }
        drop(volumes);

        // Split the write into chunk-aligned pieces.
        let mut pos = 0usize;
        let mut vol_offset = offset;

        while pos < data.len() {
            let chunk_idx = vol_offset / CHUNK_SIZE as u64;
            let offset_in_chunk = (vol_offset % CHUNK_SIZE as u64) as usize;
            let remaining_in_chunk = CHUNK_SIZE - offset_in_chunk;
            let to_write = std::cmp::min(remaining_in_chunk, data.len() - pos);

            // Build the full chunk data.
            let chunk_data = if offset_in_chunk == 0 && to_write == CHUNK_SIZE {
                // Full chunk write — no read-modify-write needed.
                data[pos..pos + to_write].to_vec()
            } else {
                // Partial chunk write — read existing chunk, merge.
                let volumes = self.volumes.lock().unwrap();
                let vol = volumes.get(name).ok_or_else(|| {
                    DataPlaneError::BdevError("volume disappeared during write".into())
                })?;
                let existing_id = vol.chunks.read().unwrap().get(&chunk_idx).cloned();
                drop(volumes);

                let mut buf = if let Some(ref cid) = existing_id {
                    self.store.read_chunk(cid)?
                } else {
                    vec![0u8; CHUNK_SIZE]
                };
                // Ensure buffer is full chunk size.
                buf.resize(CHUNK_SIZE, 0);
                buf[offset_in_chunk..offset_in_chunk + to_write]
                    .copy_from_slice(&data[pos..pos + to_write]);
                buf
            };

            // Write the chunk to the store (content-addressed, dedup built in).
            let chunk_id = self.store.write_chunk(&chunk_data)?;

            // Update the volume's chunk index.
            let volumes = self.volumes.lock().unwrap();
            if let Some(vol) = volumes.get(name) {
                vol.chunks.write().unwrap().insert(chunk_idx, chunk_id);
            }
            drop(volumes);

            pos += to_write;
            vol_offset += to_write as u64;
        }

        Ok(())
    }

    fn create_snapshot(&self, volume_name: &str, snapshot_name: &str) -> Result<SnapshotInfo> {
        info!("chunk: snapshot '{}' of '{}'", snapshot_name, volume_name);
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes.get(volume_name).ok_or_else(|| {
            DataPlaneError::BdevError(format!("volume '{}' not found", volume_name))
        })?;

        // Instant snapshot: just copy the chunk index. Chunks are immutable,
        // so no data needs to be copied.
        let chunk_index = vol.chunks.read().unwrap().clone();
        let size = vol.size_bytes;
        let used = Self::used_bytes(&chunk_index);
        drop(volumes);

        let created_at = Self::now_epoch();

        self.snapshots.lock().unwrap().insert(
            snapshot_name.to_string(),
            ChunkSnapshot {
                name: snapshot_name.to_string(),
                source_volume: volume_name.to_string(),
                chunks: chunk_index,
                size_bytes: size,
                created_at,
            },
        );

        Ok(SnapshotInfo {
            name: snapshot_name.to_string(),
            source_volume: volume_name.to_string(),
            size_bytes: size,
            used_bytes: used,
            created_at,
        })
    }

    fn delete_snapshot(&self, snapshot_name: &str) -> Result<()> {
        info!("chunk: deleting snapshot '{}'", snapshot_name);

        // Check no clones reference this snapshot.
        let volumes = self.volumes.lock().unwrap();
        for vol in volumes.values() {
            if vol.parent_snapshot.as_deref() == Some(snapshot_name) {
                return Err(DataPlaneError::BdevError(format!(
                    "snapshot '{}' has active clone '{}'",
                    snapshot_name, vol.name
                )));
            }
        }
        drop(volumes);

        self.snapshots
            .lock()
            .unwrap()
            .remove(snapshot_name)
            .ok_or_else(|| {
                DataPlaneError::BdevError(format!("snapshot '{}' not found", snapshot_name))
            })?;
        // Chunks are reference-counted by the store. We don't delete them here
        // because other volumes or snapshots may still reference them.
        Ok(())
    }

    fn list_snapshots(&self, volume_name: &str) -> Result<Vec<SnapshotInfo>> {
        let snapshots = self.snapshots.lock().unwrap();
        Ok(snapshots
            .values()
            .filter(|s| s.source_volume == volume_name)
            .map(|s| SnapshotInfo {
                name: s.name.clone(),
                source_volume: s.source_volume.clone(),
                size_bytes: s.size_bytes,
                used_bytes: Self::used_bytes(&s.chunks),
                created_at: s.created_at,
            })
            .collect())
    }

    fn clone(&self, snapshot_name: &str, clone_name: &str) -> Result<VolumeInfo> {
        info!(
            "chunk: clone '{}' from snapshot '{}'",
            clone_name, snapshot_name
        );

        let snapshots = self.snapshots.lock().unwrap();
        let snap = snapshots.get(snapshot_name).ok_or_else(|| {
            DataPlaneError::BdevError(format!("snapshot '{}' not found", snapshot_name))
        })?;

        // Instant clone: copy the chunk index. Chunks are immutable and shared.
        let chunk_index = snap.chunks.clone();
        let size = snap.size_bytes;
        let used = Self::used_bytes(&chunk_index);
        drop(snapshots);

        let vol = ChunkVolume {
            name: clone_name.to_string(),
            size_bytes: size,
            chunks: RwLock::new(chunk_index),
            is_snapshot: false,
            parent_snapshot: Some(snapshot_name.to_string()),
        };
        self.volumes
            .lock()
            .unwrap()
            .insert(clone_name.to_string(), vol);

        Ok(VolumeInfo {
            name: clone_name.to_string(),
            backend: BackendType::Chunk,
            size_bytes: size,
            used_bytes: used,
            block_size: CHUNK_SIZE as u32,
            healthy: true,
            is_snapshot: false,
            parent_snapshot: Some(snapshot_name.to_string()),
            thin_provisioned: true,
        })
    }
}

impl ChunkBackend {
    /// Check if a chunk is referenced by any volume or snapshot other than `exclude_vol`.
    fn is_chunk_referenced_elsewhere(&self, chunk_id: &str, exclude_vol: &str) -> bool {
        let volumes = self.volumes.lock().unwrap();
        for (name, vol) in volumes.iter() {
            if name == exclude_vol {
                continue;
            }
            if vol.chunks.read().unwrap().values().any(|id| id == chunk_id) {
                return true;
            }
        }
        drop(volumes);

        let snapshots = self.snapshots.lock().unwrap();
        for snap in snapshots.values() {
            if snap.chunks.values().any(|id| id == chunk_id) {
                return true;
            }
        }
        false
    }

    /// Internal read helper to avoid holding locks across store I/O.
    fn read_inner(&self, vol: &ChunkVolume, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(length as usize);
        let mut pos = offset;
        let end = offset + length;

        while pos < end {
            let chunk_idx = pos / CHUNK_SIZE as u64;
            let offset_in_chunk = (pos % CHUNK_SIZE as u64) as usize;
            let remaining_in_chunk = CHUNK_SIZE - offset_in_chunk;
            let to_read = std::cmp::min(remaining_in_chunk as u64, end - pos) as usize;

            let chunk_id = vol.chunks.read().unwrap().get(&chunk_idx).cloned();

            if let Some(cid) = chunk_id {
                let chunk_data = self.store.read_chunk(&cid)?;
                let slice_end = std::cmp::min(offset_in_chunk + to_read, chunk_data.len());
                result.extend_from_slice(&chunk_data[offset_in_chunk..slice_end]);
                if slice_end - offset_in_chunk < to_read {
                    result.resize(result.len() + (to_read - (slice_end - offset_in_chunk)), 0);
                }
            } else {
                result.resize(result.len() + to_read, 0);
            }
            pos += to_read as u64;
        }
        Ok(result)
    }
}
