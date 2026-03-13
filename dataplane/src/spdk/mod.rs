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
/// (initialising managers + starting the JSON-RPC server) and then blocks in
/// the SPDK reactor loop until `spdk_app_stop` is called (e.g. via SIGINT).
pub fn run(config: DataPlaneConfig) -> Result<()> {
    // This blocks until spdk_app_stop is called.
    // The JSON-RPC server is started from inside the startup callback.
    env::init_spdk_env(&config)?;
    env::shutdown_spdk_env();
    Ok(())
}
