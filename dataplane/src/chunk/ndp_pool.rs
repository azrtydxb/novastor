//! NDP connection pool for sub-block I/O to remote backend nodes.
//!
//! Provides lazy-connect semantics: connections are established on first use
//! and reused for subsequent requests to the same address. Each connection is
//! multiplexed (multiple in-flight requests supported by `NdpConnection`).

use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use ndp::{NdpConnection, NdpHeader, NdpOp};
use tokio::sync::Mutex;

use crate::error::{DataPlaneError, Result};

/// Pool of NDP connections for sending sub-block read/write requests to
/// remote backend nodes. Connections are created lazily on first use.
pub struct NdpPool {
    connections: Mutex<HashMap<String, Arc<NdpConnection>>>,
}

impl NdpPool {
    /// Create a new empty connection pool.
    pub fn new() -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a connection to the given address.
    /// Address format: "host:port" for TCP, "/path/to/socket" or "unix:/path" for Unix.
    pub async fn get_or_connect(&self, addr: &str) -> Result<Arc<NdpConnection>> {
        let mut conns = self.connections.lock().await;
        if let Some(conn) = conns.get(addr) {
            return Ok(conn.clone());
        }
        // Lazy connect — establish on first use.
        let conn = NdpConnection::connect(addr).await.map_err(|e| {
            DataPlaneError::TransportError(format!("NDP connect to {}: {}", addr, e))
        })?;
        let conn = Arc::new(conn);
        conns.insert(addr.to_string(), conn.clone());
        info!("NDP pool: connected to {}", addr);
        Ok(conn)
    }

    /// Read a sub-block from a remote backend node.
    ///
    /// Sends an NDP Read request with the given volume hash, byte offset, and
    /// requested length. Returns the data bytes on success.
    pub async fn sub_block_read(
        &self,
        addr: &str,
        volume_hash: u64,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>> {
        let conn = self.get_or_connect(addr).await?;

        let header = NdpHeader::request(NdpOp::Read, 0, volume_hash, offset, length);
        let resp = match conn.request(header.clone(), None).await {
            Ok(r) => r,
            Err(e) => {
                // Connection broken — remove and retry once with fresh connection.
                self.remove(addr).await;
                let conn2 = self.get_or_connect(addr).await?;
                conn2.request(header, None).await.map_err(|e2| {
                    DataPlaneError::TransportError(format!(
                        "NDP read from {} (retry): {}",
                        addr, e2
                    ))
                })?
            }
        };

        if resp.header.status != 0 {
            return Err(DataPlaneError::TransportError(format!(
                "NDP read from {} failed: status={}",
                addr, resp.header.status
            )));
        }

        resp.data.ok_or_else(|| {
            DataPlaneError::TransportError(format!("NDP read from {}: response has no data", addr))
        })
    }

    /// Write a sub-block to a remote backend node.
    ///
    /// Sends an NDP Write request with the given volume hash, byte offset, and
    /// data payload. Returns `Ok(())` on success.
    pub async fn sub_block_write(
        &self,
        addr: &str,
        volume_hash: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let conn = self.get_or_connect(addr).await?;

        let header = NdpHeader::request(NdpOp::Write, 0, volume_hash, offset, data.len() as u32);
        let payload = data.to_vec();
        let resp = match conn.request(header.clone(), Some(payload.clone())).await {
            Ok(r) => r,
            Err(_) => {
                // Connection broken — remove and retry once.
                self.remove(addr).await;
                let conn2 = self.get_or_connect(addr).await?;
                conn2.request(header, Some(payload)).await.map_err(|e2| {
                    DataPlaneError::TransportError(format!("NDP write to {} (retry): {}", addr, e2))
                })?
            }
        };

        if resp.header.status != 0 {
            return Err(DataPlaneError::TransportError(format!(
                "NDP write to {} failed: status={}",
                addr, resp.header.status
            )));
        }

        Ok(())
    }

    /// Remove a connection from the pool (e.g., on topology change or node failure).
    pub async fn remove(&self, addr: &str) {
        if self.connections.lock().await.remove(addr).is_some() {
            info!("NDP pool: removed connection to {}", addr);
        }
    }

    /// Pre-warm connections to all given addresses.
    /// Called on topology update so that subsequent I/O and registration
    /// don't pay the TCP connect latency.
    pub async fn warm_connections(&self, addrs: &[String]) {
        let mut tasks = tokio::task::JoinSet::new();
        for addr in addrs {
            let a = addr.clone();
            // Check if already connected before spawning.
            let already = {
                let conns = self.connections.lock().await;
                conns.contains_key(&a)
            };
            if already {
                continue;
            }
            tasks.spawn({
                let a2 = a.clone();
                async move { (a2.clone(), NdpConnection::connect(&a2).await) }
            });
        }
        let mut conns = self.connections.lock().await;
        while let Some(result) = tasks.join_next().await {
            if let Ok((addr, Ok(conn))) = result {
                conns.entry(addr.clone()).or_insert_with(|| {
                    info!("NDP pool: pre-warmed connection to {}", addr);
                    Arc::new(conn)
                });
            }
        }
    }

    /// List all connected peer addresses.
    pub async fn peers(&self) -> Vec<String> {
        self.connections.lock().await.keys().cloned().collect()
    }
}

/// A single chunk-map update entry for broadcast to peers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMapSyncEntry {
    pub volume_id: String,
    pub chunk_index: u64,
    pub dirty_bitmap: u64,
    pub placements: Vec<String>,
    pub generation: u64,
}

impl NdpPool {
    /// Broadcast chunk map updates to all connected peers.
    /// Each update is serialized as JSON in the data payload.
    /// Returns results per peer (addr, success/failure).
    pub async fn broadcast_chunk_map_sync(
        &self,
        updates: &[ChunkMapSyncEntry],
    ) -> Vec<(String, Result<()>)> {
        let data = serde_json::to_vec(updates).unwrap_or_default();
        let conns = self.connections.lock().await;
        let mut results = Vec::new();
        for (addr, conn) in conns.iter() {
            let header = NdpHeader::request(NdpOp::ChunkMapSync, 0, 0, 0, data.len() as u32);
            let result = conn
                .request(header, Some(data.clone()))
                .await
                .map(|_| ())
                .map_err(|e| {
                    DataPlaneError::TransportError(format!("ChunkMapSync to {}: {}", addr, e))
                });
            results.push((addr.clone(), result));
        }
        results
    }
}

impl NdpPool {
    /// Register a volume name on all connected NDP peers.
    /// Called during CreateVolume to eliminate the registration race where
    /// CRUSH routes I/O to a node that doesn't yet know about the volume.
    pub async fn broadcast_register_volume(&self, volume_name: &str) -> usize {
        let data = volume_name.as_bytes().to_vec();
        let conns = self.connections.lock().await;
        let mut success = 0usize;
        for (addr, conn) in conns.iter() {
            let header = NdpHeader::request(NdpOp::RegisterVolume, 0, 0, 0, data.len() as u32);
            match conn.request(header, Some(data.clone())).await {
                Ok(resp) if resp.header.status == 0 => {
                    success += 1;
                }
                Ok(resp) => {
                    log::warn!(
                        "RegisterVolume '{}' to {} failed: status={}",
                        volume_name,
                        addr,
                        resp.header.status
                    );
                }
                Err(e) => {
                    log::warn!("RegisterVolume '{}' to {} failed: {}", volume_name, addr, e);
                }
            }
        }
        success
    }
}

impl NdpPool {
    /// Register a volume on a specific peer by address.
    /// Connects if not already connected (lazy connect).
    pub async fn register_volume_on_peer(&self, addr: &str, volume_name: &str) -> Result<()> {
        let conn = self.get_or_connect(addr).await?;
        let data = volume_name.as_bytes().to_vec();
        let header = NdpHeader::request(NdpOp::RegisterVolume, 0, 0, 0, data.len() as u32);
        let resp = conn.request(header, Some(data)).await.map_err(|e| {
            DataPlaneError::TransportError(format!("RegisterVolume to {}: {}", addr, e))
        })?;
        if resp.header.status != 0 {
            return Err(DataPlaneError::TransportError(format!(
                "RegisterVolume to {} failed: status={}",
                addr, resp.header.status
            )));
        }
        Ok(())
    }
}

impl Default for NdpPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Batches chunk map updates and broadcasts every 100ms or 1000 entries.
///
/// After a successful sub-block write, the ChunkEngine enqueues a
/// `ChunkMapSyncEntry` recording which nodes acknowledged the write.
/// The batcher aggregates these and sends them to all NDP peers either
/// when the batch reaches 1000 entries or every 100ms, whichever comes
/// first.
#[derive(Clone)]
pub struct ChunkMapBatcher {
    pool: Arc<NdpPool>,
    pending: Arc<tokio::sync::Mutex<Vec<ChunkMapSyncEntry>>>,
}

impl ChunkMapBatcher {
    /// Create a new batcher that broadcasts via the given `NdpPool`.
    ///
    /// Spawns a background tokio task that flushes pending entries every
    /// 100ms. The task silently continues when the pool has no connections.
    pub fn new(pool: Arc<NdpPool>) -> Self {
        let pending: Arc<tokio::sync::Mutex<Vec<ChunkMapSyncEntry>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let pending_clone = pending.clone();
        let pool_clone = pool.clone();

        // Spawn timer-based flush every 100ms.
        crate::tokio_handle().spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                interval.tick().await;
                let batch: Vec<ChunkMapSyncEntry> = {
                    let mut p = pending_clone.lock().await;
                    if p.is_empty() {
                        continue;
                    }
                    std::mem::take(&mut *p)
                };
                let _ = pool_clone.broadcast_chunk_map_sync(&batch).await;
            }
        });

        Self { pool, pending }
    }

    /// Enqueue a chunk map entry for batched broadcast.
    ///
    /// If the batch reaches 1000 entries, it is flushed immediately in a
    /// spawned task (non-blocking for the caller).
    pub async fn enqueue(&self, entry: ChunkMapSyncEntry) {
        let mut pending = self.pending.lock().await;
        pending.push(entry);
        if pending.len() >= 1000 {
            let batch = std::mem::take(&mut *pending);
            let pool = self.pool.clone();
            tokio::spawn(async move {
                let _ = pool.broadcast_chunk_map_sync(&batch).await;
            });
        }
    }
}
