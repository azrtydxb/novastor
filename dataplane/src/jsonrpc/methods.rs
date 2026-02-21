use std::sync::OnceLock;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{
    BlobstoreConfig, LocalBdevConfig, LvolConfig, NvmfInitiatorConfig, NvmfTargetConfig,
};
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
    router.register("nvmf_set_ana_state", wrap(nvmf_set_ana_state));
    router.register("nvmf_get_ana_state", wrap(nvmf_get_ana_state));
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

fn bdev_aio_create(p: LocalBdevConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    let info = mgr.create_aio_bdev(&p)?;
    Ok(serde_json::json!({"name": info.name}))
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

fn bdev_lvol_create_lvstore(p: BlobstoreConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    let info = mgr.create_lvol_store(&p)?;
    Ok(serde_json::json!({"uuid": info.name}))
}

fn bdev_lvol_create(p: LvolConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit("bdev manager not initialised".into()))?;
    let info = mgr.create_lvol(&p)?;
    Ok(serde_json::json!({"name": info.name}))
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

fn nvmf_create_target(p: NvmfTargetConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    let info = mgr.create_target(&p)?;
    Ok(serde_json::json!({
        "nqn": info.nqn,
        "ana_group_id": info.ana_group_id,
        "ana_state": info.ana_state,
    }))
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

fn nvmf_connect_initiator(p: NvmfInitiatorConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    let info = mgr.connect_initiator(&p)?;
    Ok(serde_json::json!({"bdev_name": info.local_bdev_name}))
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
    bdev_name: String,
    volume_id: String,
}

fn nvmf_export_local(p: NvmfExportLocalParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    let info = mgr.export_local(&p.bdev_name, &p.volume_id)?;
    Ok(serde_json::json!({"nqn": info.nqn}))
}

#[derive(Deserialize)]
struct SetAnaStateParams {
    nqn: String,
    ana_group_id: u32,
    ana_state: String,
}

fn nvmf_set_ana_state(p: SetAnaStateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    mgr.set_ana_state(&p.nqn, p.ana_group_id, &p.ana_state)?;
    Ok(serde_json::json!({}))
}

#[derive(Deserialize)]
struct GetAnaStateParams {
    nqn: String,
}

fn nvmf_get_ana_state(p: GetAnaStateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit("nvmf manager not initialised".into()))?;
    let (ana_group_id, ana_state) = mgr.get_ana_state(&p.nqn)?;
    Ok(serde_json::json!({
        "ana_group_id": ana_group_id,
        "ana_state": ana_state,
    }))
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
