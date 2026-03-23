//! NDP-backed block device for the NVMe-oF target.
//!
//! Implements the `NvmeOfBackend` trait by forwarding all I/O to a chunk engine
//! via NDP. The CRUSH map determines which chunk engine to talk to.

use async_trait::async_trait;
use log::{info, warn};
use ndp::{ConnectionPool, NdpHeader, NdpOp};
use std::sync::Arc;

/// Block device that routes I/O to chunk engines via NDP.
pub struct NdpBlockDevice {
    /// Volume size in bytes.
    size: u64,
    /// Volume hash for NDP headers.
    volume_hash: u64,
    /// Connection pool to chunk engines.
    pool: Arc<ConnectionPool>,
    /// Address of the owner chunk engine (for writes).
    owner_addr: String,
}

impl NdpBlockDevice {
    pub fn new(
        volume_name: String,
        size: u64,
        pool: Arc<ConnectionPool>,
        owner_addr: String,
    ) -> Self {
        let volume_hash = ndp::header::volume_hash(&volume_name);
        info!("NdpBlockDevice: volume='{}' size={} owner={}",
            volume_name, size, owner_addr);
        Self {
            size,
            volume_hash,
            pool,
            owner_addr,
        }
    }
}

#[async_trait]
impl nvmeof_tcp::NvmeOfBackend for NdpBlockDevice {
    fn size(&self) -> u64 {
        self.size
    }

    fn block_size(&self) -> u32 {
        512
    }

    async fn read(&self, offset: u64, length: u32) -> nvmeof_tcp::Result<Vec<u8>> {
        let conn = self
            .pool
            .get(&self.owner_addr)
            .await
            .map_err(|e| nvmeof_tcp::NvmeOfError::Backend(format!("pool.get: {}", e)))?;

        let header = NdpHeader::request(NdpOp::Read, 0, self.volume_hash, offset, length);

        // Use a timeout to avoid hanging forever if the NDP connection is dead.
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            conn.request(header, None),
        )
        .await
        .map_err(|_| nvmeof_tcp::NvmeOfError::Backend(
            format!("NDP read timeout: offset={} len={} hash=0x{:016X}", offset, length, self.volume_hash)
        ))?
        .map_err(|e| nvmeof_tcp::NvmeOfError::Backend(format!("NDP request: {}", e)))?;

        if resp.header.status != 0 {
            return Err(nvmeof_tcp::NvmeOfError::Backend(format!(
                "NDP read failed: status={} offset={} len={}",
                resp.header.status, offset, length
            )));
        }

        resp.data
            .ok_or_else(|| nvmeof_tcp::NvmeOfError::Backend("NDP read: no data in response".into()))
    }

    async fn write(&self, offset: u64, data: &[u8]) -> nvmeof_tcp::Result<()> {
        let conn = self
            .pool
            .get(&self.owner_addr)
            .await
            .map_err(|e| nvmeof_tcp::NvmeOfError::Backend(format!("pool.get: {}", e)))?;

        let header =
            NdpHeader::request(NdpOp::Write, 0, self.volume_hash, offset, data.len() as u32);

        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            conn.request(header, Some(data.to_vec())),
        )
        .await
        .map_err(|_| nvmeof_tcp::NvmeOfError::Backend(
            format!("NDP write timeout: offset={} len={}", offset, data.len())
        ))?
        .map_err(|e| nvmeof_tcp::NvmeOfError::Backend(format!("NDP request: {}", e)))?;

        if resp.header.status != 0 {
            return Err(nvmeof_tcp::NvmeOfError::Backend(format!(
                "NDP write failed: status={}",
                resp.header.status
            )));
        }

        Ok(())
    }

    async fn flush(&self) -> nvmeof_tcp::Result<()> {
        Ok(())
    }
}
