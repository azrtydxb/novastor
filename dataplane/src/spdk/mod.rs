//! SPDK application lifecycle and reactor management.

pub mod bdev_manager;
pub mod context;
pub mod env;
pub mod nvmf_manager;
pub mod reactor_dispatch;

use crate::config::DataPlaneConfig;
use crate::error::Result;

/// Run the SPDK data plane application.
///
/// `init_spdk_env` calls `spdk_app_start` which invokes the startup callback
/// (initialising SPDK managers) and then blocks in the SPDK reactor loop
/// until `spdk_app_stop` is called (e.g. via SIGINT). The gRPC
/// DataplaneService is started separately by the transport layer.
pub fn run(config: DataPlaneConfig) -> Result<()> {
    // This blocks until spdk_app_stop is called.
    env::init_spdk_env(&config)?;
    env::shutdown_spdk_env();
    Ok(())
}
