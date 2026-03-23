//! Volume manager — creates and manages NVMe-oF volumes.
//!
//! All volumes are served through a single NVMe-oF TCP listener.
//! Each volume is identified by its NQN during the Fabric Connect command.
//! I/O is routed to chunk engines via NDP.

use log::info;
use ndp::ConnectionPool;
use nvmeof_tcp::{NvmeOfTarget, SubsystemConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::ndp_backend::NdpBlockDevice;

/// Manages active volumes behind a single NVMe-oF TCP target.
pub struct VolumeManager {
    pool: Arc<ConnectionPool>,
    target: Arc<NvmeOfTarget>,
    nvmeof_port: u16,
    listen_addr: String,
    listener_started: Mutex<bool>,
}

impl VolumeManager {
    pub fn new(pool: Arc<ConnectionPool>, nvmeof_port: u16, listen_addr: String) -> Self {
        Self {
            pool,
            target: Arc::new(NvmeOfTarget::new()),
            nvmeof_port,
            listen_addr,
            listener_started: Mutex::new(false),
        }
    }

    /// Start the shared NVMe-oF TCP listener (called once).
    async fn ensure_listener(&self) -> Result<(), String> {
        let mut started = self.listener_started.lock().await;
        if *started {
            return Ok(());
        }

        let bind_addr: std::net::SocketAddr = format!("{}:{}", self.listen_addr, self.nvmeof_port)
            .parse()
            .map_err(|e| format!("parse NVMe-oF addr: {}", e))?;
        let socket = socket2::Socket::new(
            if bind_addr.is_ipv6() { socket2::Domain::IPV6 } else { socket2::Domain::IPV4 },
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        ).map_err(|e| format!("socket: {}", e))?;
        socket.set_reuse_address(true).ok();
        #[cfg(unix)]
        socket.set_reuse_port(true).ok();
        socket.bind(&bind_addr.into()).map_err(|e| format!("bind NVMe-oF {}: {}", bind_addr, e))?;
        socket.listen(128).map_err(|e| format!("listen: {}", e))?;
        socket.set_nonblocking(true).map_err(|e| format!("nonblocking: {}", e))?;
        let listener = TcpListener::from_std(socket.into())
            .map_err(|e| format!("from_std: {}", e))?;

        info!("NVMe-oF target listening on {}", bind_addr);

        let target = self.target.clone();
        tokio::spawn(async move {
            target.serve(listener).await;
        });

        *started = true;
        Ok(())
    }

    /// Create a volume and register it with the shared NVMe-oF target.
    pub async fn create_volume(
        &self,
        volume_id: &str,
        size_bytes: u64,
        owner_addr: &str,
    ) -> Result<String, String> {
        // Ensure the listener is running.
        self.ensure_listener().await?;

        let nqn = format!("nqn.2024-01.io.novastor:volume-{}", volume_id);

        // Create the NDP-backed block device.
        let backend = NdpBlockDevice::new(
            volume_id.to_string(),
            size_bytes,
            self.pool.clone(),
            owner_addr.to_string(),
        );

        let config = SubsystemConfig {
            nqn: nqn.clone(),
            serial: format!("NOVA{:.16}", volume_id),
            model: "NovaStor Volume".to_string(),
        };

        // Register the subsystem with the shared target.
        self.target.add_subsystem(config, backend);

        info!(
            "Volume '{}': registered on NVMe-oF target ← NDP → chunk engine {}",
            volume_id, owner_addr
        );

        Ok(nqn)
    }

    /// Get the owner chunk engine address for a volume.
    pub async fn get_owner_addr(&self, _volume_id: &str) -> Result<String, String> {
        let peers = self.pool.peers().await;
        peers
            .into_iter()
            .next()
            .ok_or_else(|| "no chunk engines connected".to_string())
    }

    /// Delete a volume.
    pub async fn delete_volume(&self, volume_id: &str) -> Result<(), String> {
        let nqn = format!("nqn.2024-01.io.novastor:volume-{}", volume_id);
        self.target.remove_subsystem(&nqn);
        info!("Volume '{}' deleted (NQN: {})", volume_id, nqn);
        Ok(())
    }
}
