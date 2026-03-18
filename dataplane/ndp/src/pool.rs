use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::connection::NdpConnection;
use crate::error::{NdpError, Result};

/// Pool of persistent NDP connections to multiple chunk engines.
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<String, Arc<NdpConnection>>>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a persistent connection to a peer.
    pub async fn add(&self, addr: &str) -> Result<()> {
        let conn = NdpConnection::connect(addr).await?;
        self.connections
            .write()
            .await
            .insert(addr.to_string(), Arc::new(conn));
        Ok(())
    }

    /// Get a connection to a peer.
    pub async fn get(&self, addr: &str) -> Result<Arc<NdpConnection>> {
        self.connections
            .read()
            .await
            .get(addr)
            .cloned()
            .ok_or_else(|| NdpError::Pool(format!("no connection to {}", addr)))
    }

    /// Remove a connection.
    pub async fn remove(&self, addr: &str) {
        self.connections.write().await.remove(addr);
    }

    /// List all connected peers.
    pub async fn peers(&self) -> Vec<String> {
        self.connections.read().await.keys().cloned().collect()
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}
