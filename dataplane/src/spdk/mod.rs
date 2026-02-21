//! SPDK application lifecycle and reactor management.

pub mod bdev_manager;
pub mod env;
pub mod nvmf_manager;

use std::sync::Arc;

use crate::config::DataPlaneConfig;
use crate::error::Result;
use crate::jsonrpc;
use crate::jsonrpc::server::Router;
use log::info;

/// Run the SPDK data plane application.
pub fn run(config: DataPlaneConfig) -> Result<()> {
    let rpc_socket = config.rpc_socket.clone();

    env::init_spdk_env(&config)?;

    // Initialise managers and register RPC methods.
    let bdev_mgr = bdev_manager::BdevManager::new();
    let nvmf_mgr = nvmf_manager::NvmfManager::new(config.listen_port);
    jsonrpc::methods::init_managers(bdev_mgr, nvmf_mgr);

    let mut router = Router::new();
    jsonrpc::methods::register_all(&mut router);
    let router = Arc::new(router);

    info!("starting JSON-RPC server on {}", rpc_socket);
    let _rpc_handle = jsonrpc::server::start_server(&rpc_socket, router)?;

    info!("entering SPDK reactor loop");
    env::run_reactor_loop()?;

    env::shutdown_spdk_env();
    Ok(())
}
