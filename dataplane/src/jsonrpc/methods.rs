use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::bdev::replica::ReplicaBdev;
use crate::config::{
    BlobstoreConfig, LocalBdevConfig, LvolConfig, NvmfInitiatorConfig, NvmfTargetConfig,
    ReadPolicy, ReplicaBdevConfig, ReplicaTarget,
};
use crate::error::DataPlaneError;
use crate::spdk::bdev_manager::BdevManager;
use crate::spdk::nvmf_manager::NvmfManager;
use super::server::{MethodHandler, Router};

/// Global bdev manager, initialised once during startup.
static BDEV_MANAGER: OnceLock<BdevManager> = OnceLock::new();

/// Global NVMe-oF manager, initialised once during startup.
static NVMF_MANAGER: OnceLock<NvmfManager> = OnceLock::new();

/// Global registry of replica bdevs, keyed by volume ID.
static REPLICA_BDEVS: OnceLock<Mutex<HashMap<String, Arc<ReplicaBdev>>>> = OnceLock::new();

fn replica_bdevs() -> &'static Mutex<HashMap<String, Arc<ReplicaBdev>>> {
    REPLICA_BDEVS.get_or_init(|| Mutex::new(HashMap::new()))
}

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
    router.register("replica_bdev_status", wrap(handle_replica_status));
    router.register("replica_bdev_add_replica", wrap(handle_replica_add));
    router.register("replica_bdev_remove_replica", wrap(handle_replica_remove));
    router.register("novastor_io_stats", wrap(handle_io_stats));
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
    let read_policy = match p.read_policy.as_deref() {
        Some("local_first") => {
            let local_addr = p.targets.iter()
                .find(|t| t.is_local)
                .map(|t| t.addr.clone())
                .unwrap_or_default();
            ReadPolicy::LocalFirst { local_address: local_addr }
        }
        Some("latency_aware") => ReadPolicy::LatencyAware,
        _ => ReadPolicy::RoundRobin,
    };

    let replicas: Vec<ReplicaTarget> = p.targets.iter().map(|t| ReplicaTarget {
        address: t.addr.clone(),
        port: t.port,
        nqn: t.nqn.clone(),
    }).collect();

    let replica_count = replicas.len();
    let write_quorum = (replica_count as u32 / 2) + 1;

    let config = ReplicaBdevConfig {
        volume_id: p.name.clone(),
        replicas,
        write_quorum,
        read_policy,
    };

    let bdev = Arc::new(ReplicaBdev::new(config));
    let bdev_name = bdev.bdev_name.clone();

    // Store in the global registry
    let mut registry = replica_bdevs().lock().unwrap();
    registry.insert(p.name.clone(), bdev);

    Ok(serde_json::json!({
        "name": bdev_name,
        "replica_count": replica_count,
    }))
}

// ---------------------------------------------------------------------------
// Replica add / remove / status handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ReplicaAddParams {
    volume_id: String,
    target: ReplicaTargetAddParam,
}

#[derive(Deserialize)]
struct ReplicaTargetAddParam {
    addr: String,
    port: u16,
    nqn: String,
}

fn handle_replica_add(p: ReplicaAddParams) -> Result<serde_json::Value, DataPlaneError> {
    let registry = replica_bdevs().lock().unwrap();
    let bdev = registry.get(&p.volume_id).ok_or_else(|| {
        DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
    })?.clone();
    drop(registry);

    let target = ReplicaTarget {
        address: p.target.addr,
        port: p.target.port,
        nqn: p.target.nqn,
    };
    bdev.add_replica(target)?;

    let count = bdev.replicas.read().unwrap().len();
    Ok(serde_json::json!({
        "volume_id": p.volume_id,
        "replica_count": count,
    }))
}

#[derive(Deserialize)]
struct ReplicaRemoveParams {
    volume_id: String,
    address: String,
}

fn handle_replica_remove(p: ReplicaRemoveParams) -> Result<serde_json::Value, DataPlaneError> {
    let registry = replica_bdevs().lock().unwrap();
    let bdev = registry.get(&p.volume_id).ok_or_else(|| {
        DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
    })?.clone();
    drop(registry);

    bdev.remove_replica(&p.address)?;

    let count = bdev.replicas.read().unwrap().len();
    Ok(serde_json::json!({
        "volume_id": p.volume_id,
        "replica_count": count,
    }))
}

#[derive(Deserialize)]
struct ReplicaStatusParams {
    volume_id: String,
}

fn handle_replica_status(p: ReplicaStatusParams) -> Result<serde_json::Value, DataPlaneError> {
    let registry = replica_bdevs().lock().unwrap();
    let bdev = registry.get(&p.volume_id).ok_or_else(|| {
        DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
    })?.clone();
    drop(registry);

    let status = bdev.full_status();
    let replica_infos: Vec<serde_json::Value> = status.replicas.iter().map(|r| {
        serde_json::json!({
            "address": r.address,
            "port": r.port,
            "state": format!("{:?}", r.state),
            "reads_completed": r.reads_completed,
            "writes_completed": r.writes_completed,
            "read_errors": r.read_errors,
            "write_errors": r.write_errors,
            "read_bytes": r.read_bytes,
            "avg_read_latency_us": r.avg_read_latency_us,
        })
    }).collect();

    Ok(serde_json::json!({
        "volume_id": status.volume_id,
        "replicas": replica_infos,
        "write_quorum": status.write_quorum,
        "healthy_count": status.healthy_count,
        "total_read_iops": status.total_read_iops,
        "total_write_iops": status.total_write_iops,
        "write_quorum_latency_us": status.write_quorum_latency_us,
    }))
}

// ---------------------------------------------------------------------------
// I/O statistics
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct IoStatsParams {
    volume_id: String,
}

fn handle_io_stats(p: IoStatsParams) -> Result<serde_json::Value, DataPlaneError> {
    let registry = replica_bdevs().lock().unwrap();
    let bdev = registry.get(&p.volume_id).ok_or_else(|| {
        DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
    })?.clone();
    drop(registry);

    let stats = bdev.io_stats();
    let replica_stats: Vec<serde_json::Value> = stats.replicas.iter().map(|r| {
        serde_json::json!({
            "addr": r.address,
            "port": r.port,
            "reads_completed": r.reads_completed,
            "read_bytes": r.read_bytes,
            "avg_read_latency_us": r.avg_read_latency_us,
        })
    }).collect();

    Ok(serde_json::json!({
        "volume_id": stats.volume_id,
        "replicas": replica_stats,
        "total_read_iops": stats.total_read_iops,
        "total_write_iops": stats.total_write_iops,
        "write_quorum_latency_us": stats.write_quorum_latency_us,
    }))
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
