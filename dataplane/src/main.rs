//! NovaStor SPDK Data Plane
//!
//! High-performance storage data plane built on SPDK. Provides NVMe-oF/TCP
//! targets with custom bdev modules for replication and erasure coding.
//! Controlled by the Go agent via JSON-RPC over a Unix domain socket.

mod bdev;
mod config;
mod error;
mod jsonrpc;
mod spdk;

use clap::Parser;
use log::{error, info};

#[derive(Parser, Debug)]
#[command(name = "novastor-dataplane", version, about)]
struct Args {
    /// JSON-RPC Unix socket path
    #[arg(long, default_value = "/var/tmp/novastor-spdk.sock")]
    rpc_socket: String,

    /// SPDK reactor CPU mask (hex)
    #[arg(long, default_value = "0x1")]
    reactor_mask: String,

    /// Hugepage memory in MB
    #[arg(long, default_value_t = 2048)]
    mem_size: u32,

    /// NVMe-oF transport type
    #[arg(long, default_value = "TCP")]
    transport_type: String,

    /// NVMe-oF listen address
    #[arg(long, default_value = "0.0.0.0")]
    listen_address: String,

    /// NVMe-oF listen port
    #[arg(long, default_value_t = 4420)]
    listen_port: u16,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

fn main() {
    let args = Args::parse();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(&args.log_level),
    )
    .init();

    info!(
        "novastor-dataplane starting (reactor_mask={}, mem={}MB)",
        args.reactor_mask, args.mem_size
    );

    let config = config::DataPlaneConfig {
        rpc_socket: args.rpc_socket,
        reactor_mask: args.reactor_mask,
        mem_size: args.mem_size,
        transport_type: args.transport_type,
        listen_address: args.listen_address,
        listen_port: args.listen_port,
    };

    if let Err(e) = spdk::run(config) {
        error!("data plane failed: {}", e);
        std::process::exit(1);
    }
    info!("novastor-dataplane stopped");
}
