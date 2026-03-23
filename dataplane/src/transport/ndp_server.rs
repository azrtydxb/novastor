//! NDP server — accepts connections from frontend controllers and serves
//! sub-block I/O via the NovaStor Data Protocol.
//!
//! Runs on tokio (not the SPDK reactor). Receives NDP messages, dispatches
//! reads/writes to the sub-block I/O functions, and sends responses.

use log::{error, info, warn};
use ndp::{NdpHeader, NdpMessage, NdpOp};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::bdev::novastor_bdev;
use crate::bdev::sub_block;
use crate::error::Result;

/// Default Unix socket path for local NDP connections.
pub const NDP_UNIX_SOCKET: &str = "/var/run/novastor/ndp.sock";

/// Configuration for the NDP server.
pub struct NdpServerConfig {
    pub listen_address: String,
    pub port: u16,
    pub unix_socket: Option<String>,
}

impl Default for NdpServerConfig {
    fn default() -> Self {
        Self {
            listen_address: "::".to_string(),
            port: 4500,
            unix_socket: Some(NDP_UNIX_SOCKET.to_string()),
        }
    }
}

/// Start the NDP server. Listens on both TCP (for remote) and Unix socket (for local).
pub async fn start(config: NdpServerConfig) -> Result<()> {
    let handle = crate::tokio_handle().clone();

    // Start TCP listener
    let addr = format!("{}:{}", config.listen_address, config.port);
    let tcp_listener = TcpListener::bind(&addr).await.map_err(|e| {
        crate::error::DataPlaneError::TransportError(format!("NDP bind {}: {}", addr, e))
    })?;
    info!("NDP server listening on {} (TCP)", addr);

    handle.spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok((stream, peer)) => {
                    info!("NDP: TCP connection from {}", peer);
                    stream.set_nodelay(true).ok();
                    crate::tokio_handle().spawn(handle_connection_generic(stream));
                }
                Err(e) => {
                    error!("NDP TCP accept error: {}", e);
                }
            }
        }
    });

    // Start Unix socket listener (for low-latency local connections)
    if let Some(socket_path) = config.unix_socket {
        let _ = std::fs::create_dir_all("/var/run/novastor");
        let _ = std::fs::remove_file(&socket_path);
        let unix_listener = tokio::net::UnixListener::bind(&socket_path).map_err(|e| {
            crate::error::DataPlaneError::TransportError(format!(
                "NDP unix bind {}: {}",
                socket_path, e
            ))
        })?;
        info!("NDP server listening on {} (Unix)", socket_path);

        crate::tokio_handle().spawn(async move {
            loop {
                match unix_listener.accept().await {
                    Ok((stream, _)) => {
                        info!("NDP: Unix connection");
                        crate::tokio_handle().spawn(handle_connection_generic(stream));
                    }
                    Err(e) => {
                        error!("NDP Unix accept error: {}", e);
                    }
                }
            }
        });
    }

    Ok(())
}

async fn handle_connection_generic<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
>(
    stream: S,
) {
    let (mut reader, writer) = tokio::io::split(stream);
    let writer = Arc::new(tokio::sync::Mutex::new(writer));

    loop {
        let msg = match NdpMessage::read_from(&mut reader).await {
            Ok(m) => m,
            Err(ndp::NdpError::ConnectionClosed) => break,
            Err(e) => {
                warn!("NDP read error: {}", e);
                break;
            }
        };

        // Process each request concurrently — don't block the reader.
        let w = writer.clone();
        crate::tokio_handle().spawn(async move {
            let response = handle_request(msg).await;
            let mut writer = w.lock().await;
            if let Err(e) = response.write_to(&mut *writer).await {
                warn!("NDP write error: {}", e);
            }
            if let Err(e) = writer.flush().await {
                warn!("NDP flush error: {}", e);
            }
        });
    }
}

async fn handle_request(msg: NdpMessage) -> NdpMessage {
    match msg.header.op {
        NdpOp::Ping => NdpMessage::new(NdpHeader::response(&msg.header, NdpOp::Pong, 0, 0), None),

        NdpOp::Read => handle_read(&msg.header).await,

        NdpOp::Write => handle_write(&msg.header, msg.data).await,

        NdpOp::WriteZeroes | NdpOp::Unmap => {
            // Thin provisioning — no-op, same as bdev handler.
            NdpMessage::new(
                NdpHeader::response(&msg.header, NdpOp::WriteResp, 0, 0),
                None,
            )
        }

        NdpOp::Replicate => handle_write(&msg.header, msg.data).await,

        _ => {
            // Unknown or response op received as request.
            NdpMessage::new(
                NdpHeader::response(&msg.header, NdpOp::WriteResp, 1, 0),
                None,
            )
        }
    }
}

async fn handle_read(header: &NdpHeader) -> NdpMessage {
    let volume_name = match lookup_volume(header.volume_hash) {
        Some(name) => name,
        None => {
            return NdpMessage::new(
                NdpHeader::response(header, NdpOp::ReadResp, 2, 0), // status 2 = not found
                None,
            );
        }
    };

    // Use the existing sub_block_read path.
    match crate::bdev::novastor_bdev::sub_block_read_pub(
        &volume_name,
        header.offset,
        header.data_length as u64,
    )
    .await
    {
        Ok(data) => {
            let mut resp = NdpHeader::response(header, NdpOp::ReadResp, 0, data.len() as u32);
            NdpMessage::new(resp, Some(data))
        }
        Err(e) => {
            warn!("NDP read error for volume {}: {}", volume_name, e);
            NdpMessage::new(
                NdpHeader::response(header, NdpOp::ReadResp, 3, 0), // status 3 = I/O error
                None,
            )
        }
    }
}

async fn handle_write(header: &NdpHeader, data: Option<Vec<u8>>) -> NdpMessage {
    let volume_name = match lookup_volume(header.volume_hash) {
        Some(name) => name,
        None => {
            return NdpMessage::new(NdpHeader::response(header, NdpOp::WriteResp, 2, 0), None);
        }
    };

    let write_data = match data {
        Some(d) => d,
        None => {
            return NdpMessage::new(
                NdpHeader::response(header, NdpOp::WriteResp, 4, 0), // status 4 = missing data
                None,
            );
        }
    };

    match crate::bdev::novastor_bdev::sub_block_write_pub(&volume_name, header.offset, &write_data)
        .await
    {
        Ok(()) => NdpMessage::new(NdpHeader::response(header, NdpOp::WriteResp, 0, 0), None),
        Err(e) => {
            warn!("NDP write error for volume {}: {}", volume_name, e);
            NdpMessage::new(NdpHeader::response(header, NdpOp::WriteResp, 3, 0), None)
        }
    }
}

/// Reverse-lookup volume name from volume_hash.
/// Scans the bdev registry for a matching hash.
fn lookup_volume(hash: u64) -> Option<String> {
    // Check the volume hash cache first.
    if let Some(name) = VOLUME_HASH_CACHE
        .get()
        .and_then(|c| c.read().ok().and_then(|map| map.get(&hash).cloned()))
    {
        return Some(name);
    }
    None
}

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

static VOLUME_HASH_CACHE: OnceLock<RwLock<HashMap<u64, String>>> = OnceLock::new();

/// Register a volume name → hash mapping for NDP lookups.
pub fn register_volume_hash(volume_name: &str) {
    let hash = ndp::header::volume_hash(volume_name);
    let cache = VOLUME_HASH_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    cache.write().unwrap().insert(hash, volume_name.to_string());
    info!(
        "NDP: registered volume '{}' with hash 0x{:016X}",
        volume_name, hash
    );
}

/// Unregister a volume hash.
pub fn unregister_volume_hash(volume_name: &str) {
    let hash = ndp::header::volume_hash(volume_name);
    if let Some(cache) = VOLUME_HASH_CACHE.get() {
        cache.write().unwrap().remove(&hash);
    }
}
