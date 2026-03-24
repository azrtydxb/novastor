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
    async fn get_or_connect(&self, addr: &str) -> Result<Arc<NdpConnection>> {
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
        let resp = conn.request(header, None).await.map_err(|e| {
            DataPlaneError::TransportError(format!("NDP read from {}: {}", addr, e))
        })?;

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
        let resp = conn
            .request(header, Some(data.to_vec()))
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("NDP write to {}: {}", addr, e)))?;

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

    /// List all connected peer addresses.
    pub async fn peers(&self) -> Vec<String> {
        self.connections.lock().await.keys().cloned().collect()
    }
}

impl Default for NdpPool {
    fn default() -> Self {
        Self::new()
    }
}
