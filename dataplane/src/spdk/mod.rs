//! SPDK application lifecycle and reactor management.

pub mod bdev_manager;
pub mod env;
pub mod nvmf_manager;

use crate::config::DataPlaneConfig;
use crate::error::Result;

/// Run the SPDK data plane application.
///
/// In SPDK mode (`spdk-sys` feature), `init_spdk_env` calls `spdk_app_start`
/// which invokes the startup callback and then blocks in the reactor loop.
/// The callback initialises managers and starts the JSON-RPC server before
/// the reactor takes over.
///
/// In stub mode (no `spdk-sys`), `init_spdk_env` returns immediately and we
/// initialise managers, start the JSON-RPC server, and enter a simple
/// Ctrl-C–driven wait loop here.
pub fn run(config: DataPlaneConfig) -> Result<()> {
    // In SPDK mode this blocks until spdk_app_stop is called (e.g. SIGINT).
    // The JSON-RPC server is started from inside the startup callback.
    env::init_spdk_env(&config)?;

    // Stub mode: init_spdk_env returned immediately — set up JSON-RPC and
    // enter the stub reactor loop.
    #[cfg(not(feature = "spdk-sys"))]
    {
        use std::sync::Arc;

        use crate::jsonrpc;
        use crate::jsonrpc::server::Router;
        use log::info;

        let bdev_mgr = bdev_manager::BdevManager::new();
        let nvmf_mgr = nvmf_manager::NvmfManager::new(config.listen_port);
        jsonrpc::methods::init_managers(bdev_mgr, nvmf_mgr);

        let mut router = Router::new();
        jsonrpc::methods::register_all(&mut router);
        let router = Arc::new(router);

        info!("starting JSON-RPC server on {}", config.rpc_socket);
        let _rpc_handle = jsonrpc::server::start_server(&config.rpc_socket, router)?;

        info!("entering SPDK reactor loop");
        env::run_reactor_loop()?;
    }

    env::shutdown_spdk_env();
    Ok(())
}
