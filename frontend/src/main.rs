//! NovaStor Frontend Controller.
//!
//! Lightweight Rust process that serves volumes to pods via NVMe-oF TCP.
//! Routes I/O to chunk engines via NDP based on CRUSH placement.
//!
//! Architecture:
//!   Pod → NVMe-oF TCP → Frontend (this binary, userspace target) → NDP → Chunk Engines

use clap::Parser;
use log::info;
use std::sync::Arc;

mod grpc_server;
mod ndp_backend;
mod volume_manager;

use grpc_server::nvme::nv_me_target_service_server::NvMeTargetServiceServer;

#[derive(Parser, Debug)]
#[command(name = "novastor-frontend", version, about = "NovaStor Frontend Controller")]
struct Args {
    /// NDP port on chunk engines
    #[arg(long, default_value_t = 4500)]
    ndp_port: u16,

    /// NVMe-oF TCP listen port for volumes
    #[arg(long, default_value_t = 4431)]
    nvmeof_port: u16,

    /// Comma-separated list of chunk engine addresses (host:port)
    #[arg(long, default_value = "")]
    chunk_engines: String,

    /// Listen address for gRPC control plane
    #[arg(long, default_value = "0.0.0.0:9601")]
    control_addr: String,

    /// Advertised IP address for NVMe-oF targets
    #[arg(long, default_value = "0.0.0.0")]
    advertise_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    info!("novastor-frontend starting");
    info!("  NDP port: {}", args.ndp_port);
    info!("  NVMe-oF port: {}", args.nvmeof_port);
    info!("  Control plane: {}", args.control_addr);

    // Connect to chunk engines via NDP.
    let pool = Arc::new(ndp::ConnectionPool::new());
    if !args.chunk_engines.is_empty() {
        for addr in args.chunk_engines.split(',') {
            let addr = addr.trim();
            if addr.is_empty() {
                continue;
            }
            // Retry connection to chunk engine — the dataplane may start after us.
            let mut connected = false;
            for attempt in 1..=30 {
                match pool.add(addr).await {
                    Ok(()) => {
                        info!("Connected to chunk engine at {}", addr);
                        connected = true;
                        break;
                    }
                    Err(e) => {
                        if attempt < 30 {
                            log::warn!("Failed to connect to {} (attempt {}): {}", addr, attempt, e);
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        } else {
                            log::error!("Failed to connect to {} after {} attempts: {}", addr, attempt, e);
                        }
                    }
                }
            }
        }
    }

    // Create the volume manager.
    let volume_manager = Arc::new(volume_manager::VolumeManager::new(
        pool.clone(),
        args.nvmeof_port,
        args.advertise_addr.clone(),
    ));

    // Start the gRPC control plane.
    let grpc_service = grpc_server::FrontendTargetService::new(
        volume_manager,
        args.advertise_addr,
        args.nvmeof_port,
    );

    let addr: std::net::SocketAddr = args.control_addr.parse()?;

    // Bind with SO_REUSEADDR so restarts don't fail on TIME_WAIT sockets.
    let socket = socket2::Socket::new(
        if addr.is_ipv6() { socket2::Domain::IPV6 } else { socket2::Domain::IPV4 },
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    socket.listen(128)?;
    socket.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(socket.into())?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    info!("novastor-frontend ready, gRPC listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(NvMeTargetServiceServer::new(grpc_service))
        .serve_with_incoming_shutdown(incoming, async {
            tokio::signal::ctrl_c().await.ok();
            info!("novastor-frontend shutting down");
        })
        .await?;

    Ok(())
}
