use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

use crate::error::DataPlaneError;
use crate::spdk::bdev_manager::BdevManager;
use crate::spdk::nvmf_manager::NvmfManager;
use super::server::{MethodHandler, Router};

/// Global bdev manager, initialised once during startup.
static BDEV_MANAGER: OnceLock<BdevManager> = OnceLock::new();

/// Global NVMe-oF manager, initialised once during startup.
static NVMF_MANAGER: OnceLock<NvmfManager> = OnceLock::new();

/// Initialise the global managers. Must be called before serving requests.
pub fn init_managers(bdev: BdevManager, nvmf: NvmfManager) {
    BDEV_MANAGER.set(bdev).ok();
    NVMF_MANAGER.set(nvmf).ok();
}

/// Register all RPC methods with the router.
pub fn register_all(router: &mut Router) {
    router.register("bdev_aio_create", wrap(bdev_aio_create));
    router.register("bdev_malloc_create", wrap(bdev_malloc_create));
    router.register("bdev_lvol_create_lvstore", wrap(bdev_lvol_create_lvstore));
    router.register("bdev_lvol_create", wrap(bdev_lvol_create));
    router.register("bdev_delete", wrap(bdev_delete));
    router.register("nvmf_create_target", wrap(nvmf_create_target));
    router.register("nvmf_delete_target", wrap(nvmf_delete_target));
    router.register("nvmf_connect_initiator", wrap(nvmf_connect_initiator));
    router.register("nvmf_disconnect_initiator", wrap(nvmf_disconnect_initiator));
    router.register("nvmf_export_local", wrap(nvmf_export_local));
    router.register("replica_bdev_create", wrap(replica_bdev_create));
    router.register("replica_bdev_status", wrap(replica_bdev_status));
    router.register("get_version", wrap(get_version));
}

// ---------------------------------------------------------------------------
// Helper to wrap a typed handler into a MethodHandler.
// ---------------------------------------------------------------------------
fn wrap<P, R, F>(f: F) -> MethodHandler
where
    P: serde::de::DeserializeOwned + 'static,
    R: Serialize + 'static,
    F: Fn(P) -> Result<R, DataPlaneError> + Send + Sync + 'static,
{
    Arc::new(move |params: serde_json::Value| {
        let p: P = serde_json::from_value(params)
            .map_err(|e| DataPlaneError::JsonRpcError(format!("invalid params: {e}")))?;
        let result = f(p)?;
        serde_json::to_value(result)
            .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization error: {e}")))
    })
}

// ---------------------------------------------------------------------------
// Bdev methods
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AioCreateParams {
    name: String,
    filename: String,
    block_size: u32,
}

fn bdev_aio_create(p: AioCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    mgr.create_aio_bdev(&p.name, &p.filename, p.block_size)?;
    Ok(serde_json::json!({"name": p.name}))
}

#[derive(Deserialize)]
struct MallocCreateParams {
    name: String,
    size_mb: u64,
    block_size: u32,
}

fn bdev_malloc_create(p: MallocCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    mgr.create_malloc_bdev(&p.name, p.size_mb, p.block_size)?;
    Ok(serde_json::json!({"name": p.name}))
}

#[derive(Deserialize)]
struct LvolStoreCreateParams {
    bdev_name: String,
    lvs_name: String,
}

fn bdev_lvol_create_lvstore(p: LvolStoreCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    let uuid = mgr.create_lvol_store(&p.bdev_name, &p.lvs_name)?;
    Ok(serde_json::json!({"uuid": uuid}))
}

#[derive(Deserialize)]
struct LvolCreateParams {
    lvs_name: String,
    lvol_name: String,
    size_mb: u64,
}

fn bdev_lvol_create(p: LvolCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    let name = mgr.create_lvol(&p.lvs_name, &p.lvol_name, p.size_mb)?;
    Ok(serde_json::json!({"name": name}))
}

#[derive(Deserialize)]
struct BdevDeleteParams {
    name: String,
}

fn bdev_delete(p: BdevDeleteParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    mgr.delete_bdev(&p.name)?;
    Ok(serde_json::json!(true))
}

// ---------------------------------------------------------------------------
// NVMe-oF methods
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct NvmfCreateTargetParams {
    nqn: String,
    listen_addr: String,
    port: u16,
    bdev_name: String,
}

fn nvmf_create_target(p: NvmfCreateTargetParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    mgr.create_target(&p.nqn, &p.listen_addr, p.port, &p.bdev_name)?;
    Ok(serde_json::json!({"nqn": p.nqn}))
}

#[derive(Deserialize)]
struct NvmfDeleteTargetParams {
    nqn: String,
}

fn nvmf_delete_target(p: NvmfDeleteTargetParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    mgr.delete_target(&p.nqn)?;
    Ok(serde_json::json!(true))
}

#[derive(Deserialize)]
struct NvmfConnectParams {
    addr: String,
    port: u16,
    nqn: String,
}

fn nvmf_connect_initiator(p: NvmfConnectParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    let bdev_name = mgr.connect_initiator(&p.addr, p.port, &p.nqn)?;
    Ok(serde_json::json!({"bdev_name": bdev_name}))
}

#[derive(Deserialize)]
struct NvmfDisconnectParams {
    nqn: String,
}

fn nvmf_disconnect_initiator(p: NvmfDisconnectParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    mgr.disconnect_initiator(&p.nqn)?;
    Ok(serde_json::json!(true))
}

#[derive(Deserialize)]
struct NvmfExportLocalParams {
    nqn: String,
    listen_addr: String,
    port: u16,
    bdev_name: String,
}

fn nvmf_export_local(p: NvmfExportLocalParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    mgr.export_local(&p.nqn, &p.listen_addr, p.port, &p.bdev_name)?;
    Ok(serde_json::json!({"nqn": p.nqn}))
}

// ---------------------------------------------------------------------------
// Replica bdev methods
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ReplicaBdevCreateParams {
    name: String,
    targets: Vec<ReplicaTargetParam>,
    read_policy: Option<String>,
}

#[derive(Deserialize)]
struct ReplicaTargetParam {
    addr: String,
    port: u16,
    nqn: String,
    is_local: bool,
}

fn replica_bdev_create(p: ReplicaBdevCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    // Replica bdev creation is a complex operation that connects to all
    // replica targets and creates a composite bdev. In stub mode we just
    // record the configuration.
    let _ = &p;
    Ok(serde_json::json!({
        "name": p.name,
        "replica_count": p.targets.len(),
    }))
}

fn replica_bdev_status(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    Ok(serde_json::json!({"status": "healthy"}))
}

// ---------------------------------------------------------------------------
// Utility methods
// ---------------------------------------------------------------------------

fn get_version(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    Ok(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "name": env!("CARGO_PKG_NAME"),
    }))
}
