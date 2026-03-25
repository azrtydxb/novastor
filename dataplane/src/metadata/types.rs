//! Metadata types stored in the sharded Raft state machine.

use serde::{Deserialize, Serialize};

/// Data protection mode for a volume.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Protection {
    Replication {
        factor: u32,
    },
    ErasureCoding {
        data_shards: u32,
        parity_shards: u32,
    },
}

/// Lifecycle status of a volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeStatus {
    Creating,
    Available,
    Deleting,
    Error,
}

/// Full definition of a volume stored in the metadata state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeDefinition {
    pub id: String,
    pub name: String,
    pub size_bytes: u64,
    pub protection: Protection,
    pub status: VolumeStatus,
    pub created_at: u64,
    pub chunk_count: u64,
}

/// Erasure-coding parameters attached to a chunk map entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureParams {
    pub data_shards: u32,
    pub parity_shards: u32,
}

/// One entry in a volume's chunk map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapEntry {
    pub chunk_index: u64,
    pub chunk_id: String,
    pub ec_params: Option<ErasureParams>,
    /// Bitmap of which 64KB sub-blocks have been written (64 bits = 64 sub-blocks).
    #[serde(default)]
    pub dirty_bitmap: u64,
    /// Node IDs that hold a copy of this chunk (set by placement engine).
    #[serde(default)]
    pub placements: Vec<String>,
    /// Cluster map generation at the time this entry was written.
    #[serde(default)]
    pub generation: u64,
}

/// Requests that can be applied to the metadata state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetadataRequest {
    PutVolume(VolumeDefinition),
    DeleteVolume {
        volume_id: String,
    },
    PutChunkMap {
        volume_id: String,
        entry: ChunkMapEntry,
    },
    DeleteChunkMap {
        volume_id: String,
        chunk_index: u64,
    },
}

/// Responses returned by metadata state machine operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetadataResponse {
    Ok,
    Volume(VolumeDefinition),
    NotFound,
    Error { message: String },
}

/// Return the shard index (0–255) for a given volume ID.
///
/// The shard is derived from the first two hex characters of `volume_id`,
/// giving uniform distribution across UUIDs and other hex-prefixed IDs.
///
/// Returns an error if `volume_id` is empty or does not start with two
/// valid hex characters.
pub fn shard_for_volume(volume_id: &str) -> Result<u8, String> {
    if volume_id.len() < 2 {
        return Err(format!(
            "volume_id too short (need at least 2 hex chars): {:?}",
            volume_id
        ));
    }
    let hex = &volume_id[..2];
    u8::from_str_radix(hex, 16).map_err(|_| {
        format!(
            "volume_id does not start with valid hex prefix: {:?}",
            volume_id
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_definition_defaults() {
        let vol = VolumeDefinition {
            id: "abc123".to_string(),
            name: "test-volume".to_string(),
            size_bytes: 4 * 1024 * 1024 * 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Creating,
            created_at: 1_700_000_000,
            chunk_count: 1024,
        };

        assert_eq!(vol.id, "abc123");
        assert_eq!(vol.name, "test-volume");
        assert_eq!(vol.size_bytes, 4 * 1024 * 1024 * 1024);
        assert_eq!(vol.status, VolumeStatus::Creating);
        assert_eq!(vol.created_at, 1_700_000_000);
        assert_eq!(vol.chunk_count, 1024);
    }

    #[test]
    fn chunk_map_entry_roundtrip() {
        let entry = ChunkMapEntry {
            chunk_index: 7,
            chunk_id: "deadbeef".to_string(),
            ec_params: None,
            dirty_bitmap: 0,
            placements: vec![],
            generation: 0,
        };

        let json = serde_json::to_string(&entry).expect("serialize");
        let decoded: ChunkMapEntry = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.chunk_index, 7);
        assert_eq!(decoded.chunk_id, "deadbeef");
        assert!(decoded.ec_params.is_none());
    }

    #[test]
    fn ec_params_serialization() {
        let entry = ChunkMapEntry {
            chunk_index: 0,
            chunk_id: "cafebabe".to_string(),
            ec_params: Some(ErasureParams {
                data_shards: 4,
                parity_shards: 2,
            }),
            dirty_bitmap: 0,
            placements: vec![],
            generation: 0,
        };

        let json = serde_json::to_string(&entry).expect("serialize");
        let decoded: ChunkMapEntry = serde_json::from_str(&json).expect("deserialize");

        let params = decoded.ec_params.expect("ec_params present");
        assert_eq!(params.data_shards, 4);
        assert_eq!(params.parity_shards, 2);
    }

    #[test]
    fn shard_id_from_volume_id() {
        assert_eq!(shard_for_volume("ab-some-uuid").unwrap(), 0xab);
        assert_eq!(shard_for_volume("ff-some-uuid").unwrap(), 0xff);
        assert_eq!(shard_for_volume("00-some-uuid").unwrap(), 0x00);
    }

    #[test]
    fn shard_id_rejects_empty_volume_id() {
        let err = shard_for_volume("").unwrap_err();
        assert!(err.contains("too short"), "unexpected error: {err}");
    }

    #[test]
    fn shard_id_rejects_single_char_volume_id() {
        let err = shard_for_volume("a").unwrap_err();
        assert!(err.contains("too short"), "unexpected error: {err}");
    }

    #[test]
    fn shard_id_rejects_non_hex_prefix() {
        let err = shard_for_volume("zz-something").unwrap_err();
        assert!(err.contains("valid hex prefix"), "unexpected error: {err}");
    }

    #[test]
    fn metadata_request_variants() {
        let vol = VolumeDefinition {
            id: "1a2b3c".to_string(),
            name: "my-vol".to_string(),
            size_bytes: 1024,
            protection: Protection::ErasureCoding {
                data_shards: 4,
                parity_shards: 2,
            },
            status: VolumeStatus::Available,
            created_at: 0,
            chunk_count: 0,
        };

        let req = MetadataRequest::PutVolume(vol);
        let json = serde_json::to_string(&req).expect("serialize");
        let decoded: MetadataRequest = serde_json::from_str(&json).expect("deserialize");

        match decoded {
            MetadataRequest::PutVolume(v) => {
                assert_eq!(v.id, "1a2b3c");
                assert_eq!(v.status, VolumeStatus::Available);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }
}
