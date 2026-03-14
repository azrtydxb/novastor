use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use super::server::{MethodHandler, Router};
use crate::backend::chunk::ChunkBackend;
use crate::backend::lvm::LvmBackend;
use crate::backend::raw_disk::RawDiskBackend;
use crate::backend::traits::StorageBackend;
use crate::bdev::chunk_io::ChunkStore;
use crate::bdev::erasure::ErasureBdev;
use crate::bdev::novastor_bdev;
use crate::bdev::novastor_replica_bdev;
use crate::bdev::replica::ReplicaBdev;
use crate::config::{
    BlobstoreConfig, ErasureBdevConfig, LocalBdevConfig, LvolConfig, NvmfInitiatorConfig,
    NvmfTargetConfig, ReadPolicy, ReplicaBdevConfig, ReplicaTarget,
};
use crate::error::DataPlaneError;
use crate::spdk::bdev_manager::BdevManager;
use crate::spdk::nvmf_manager::NvmfManager;

/// Global bdev manager, initialised once during startup.
static BDEV_MANAGER: OnceLock<BdevManager> = OnceLock::new();

/// Global NVMe-oF manager, initialised once during startup.
static NVMF_MANAGER: OnceLock<NvmfManager> = OnceLock::new();

/// Global registry of replica bdevs, keyed by volume ID.
static REPLICA_BDEVS: OnceLock<Mutex<HashMap<String, Arc<ReplicaBdev>>>> = OnceLock::new();

/// Global registry of erasure bdevs, keyed by volume ID.
static ERASURE_BDEVS: OnceLock<Mutex<HashMap<String, Arc<ErasureBdev>>>> = OnceLock::new();

/// Global registry of chunk stores, keyed by bdev name.
static CHUNK_STORES: OnceLock<Mutex<HashMap<String, Arc<ChunkStore>>>> = OnceLock::new();

fn replica_bdevs() -> &'static Mutex<HashMap<String, Arc<ReplicaBdev>>> {
    REPLICA_BDEVS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn erasure_bdevs() -> &'static Mutex<HashMap<String, Arc<ErasureBdev>>> {
    ERASURE_BDEVS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn chunk_stores() -> &'static Mutex<HashMap<String, Arc<ChunkStore>>> {
    CHUNK_STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Global registry of storage backends, keyed by backend type string.
static BACKENDS: OnceLock<Mutex<HashMap<String, Arc<dyn StorageBackend>>>> = OnceLock::new();

fn backends() -> &'static Mutex<HashMap<String, Arc<dyn StorageBackend>>> {
    BACKENDS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_backend(backend_type: &str) -> Result<Arc<dyn StorageBackend>, DataPlaneError> {
    let reg = backends().lock().unwrap();
    reg.get(backend_type).cloned().ok_or_else(|| {
        DataPlaneError::BdevError(format!("backend '{}' not initialised", backend_type))
    })
}

/// Initialise the global managers. Must be called before serving requests.
pub fn init_managers(bdev: BdevManager, nvmf: NvmfManager) {
    BDEV_MANAGER.set(bdev).ok();
    NVMF_MANAGER.set(nvmf).ok();
}

/// Register all RPC methods with the router.
pub fn register_all(router: &mut Router) {
    // Bdev management
    router.register("bdev_aio_create", wrap(bdev_aio_create));
    router.register("bdev_malloc_create", wrap(bdev_malloc_create));
    router.register("bdev_lvol_create_lvstore", wrap(bdev_lvol_create_lvstore));
    router.register("bdev_lvol_create", wrap(bdev_lvol_create));
    router.register("bdev_delete", wrap(bdev_delete));
    router.register("bdev_list", wrap(bdev_list));
    router.register("bdev_get_info", wrap(bdev_get_info));
    router.register("bdev_lvol_list_lvstores", wrap(bdev_lvol_list_lvstores));

    // NVMe-oF management
    router.register("nvmf_init_transport", wrap(nvmf_init_transport));
    router.register("nvmf_create_target", wrap(nvmf_create_target));
    router.register("nvmf_delete_target", wrap(nvmf_delete_target));
    router.register("nvmf_connect_initiator", wrap(nvmf_connect_initiator));
    router.register("nvmf_disconnect_initiator", wrap(nvmf_disconnect_initiator));
    router.register("nvmf_export_local", wrap(nvmf_export_local));
    router.register("nvmf_set_ana_state", wrap(nvmf_set_ana_state));
    router.register("nvmf_get_ana_state", wrap(nvmf_get_ana_state));
    router.register("nvmf_list_subsystems", wrap(nvmf_list_subsystems));
    router.register("nvmf_list_initiators", wrap(nvmf_list_initiators));
    router.register("nvmf_get_subsystem", wrap(nvmf_get_subsystem));

    // Replica bdev management
    router.register("replica_bdev_create", wrap(replica_bdev_create));
    router.register("replica_bdev_status", wrap(handle_replica_status));
    router.register("replica_bdev_add_replica", wrap(handle_replica_add));
    router.register("replica_bdev_remove_replica", wrap(handle_replica_remove));
    router.register("replica_bdev_write", wrap(handle_replica_write));
    router.register("replica_bdev_read", wrap(handle_replica_read));

    // Erasure coding bdev management
    router.register("erasure_bdev_create", wrap(erasure_bdev_create));
    router.register("erasure_bdev_status", wrap(erasure_bdev_status));
    router.register("erasure_encode", wrap(erasure_encode));
    router.register("erasure_decode", wrap(erasure_decode));
    router.register("erasure_bdev_write", wrap(handle_erasure_write));
    router.register("erasure_bdev_read", wrap(handle_erasure_read));

    // Chunk I/O
    router.register("chunk_store_create", wrap(chunk_store_create));
    router.register("chunk_write", wrap(chunk_write));
    router.register("chunk_read", wrap(chunk_read));
    router.register("chunk_delete", wrap(chunk_delete));
    router.register("chunk_exists", wrap(chunk_exists));
    router.register("chunk_list", wrap(chunk_list));
    router.register("chunk_store_stats", wrap(chunk_store_stats));
    router.register("chunk_scrub", wrap(chunk_scrub));
    router.register("chunk_split_write", wrap(chunk_split_write));
    router.register("chunk_reassemble", wrap(chunk_reassemble));
    router.register("chunk_gc", wrap(chunk_gc));

    // I/O statistics
    router.register("novastor_io_stats", wrap(handle_io_stats));

    // Unified storage backends
    router.register("backend.init_raw_disk", wrap(backend_init_raw_disk));
    router.register("backend.init_lvm", wrap(backend_init_lvm));
    router.register("backend.init_chunk", wrap(backend_init_chunk));
    router.register("backend.create_volume", wrap(backend_create_volume));
    router.register("backend.delete_volume", wrap(backend_delete_volume));
    router.register("backend.resize_volume", wrap(backend_resize_volume));
    router.register("backend.stat_volume", wrap(backend_stat_volume));
    router.register("backend.list_volumes", wrap(backend_list_volumes));
    router.register("backend.read", wrap(backend_read));
    router.register("backend.write", wrap(backend_write));
    router.register("backend.create_snapshot", wrap(backend_create_snapshot));
    router.register("backend.delete_snapshot", wrap(backend_delete_snapshot));
    router.register("backend.list_snapshots", wrap(backend_list_snapshots));
    router.register("backend.clone", wrap(backend_clone));

    // Utility
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
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    let info = mgr.create_aio_bdev(&p)?;
    Ok(
        serde_json::json!({"name": info.name, "num_blocks": info.num_blocks, "block_size": info.block_size}),
    )
}

#[derive(Deserialize)]
struct MallocCreateParams {
    name: String,
    size_mb: u64,
    block_size: u32,
}

fn bdev_malloc_create(p: MallocCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    let info = mgr.create_malloc_bdev(&p.name, p.size_mb, p.block_size)?;
    Ok(
        serde_json::json!({"name": info.name, "num_blocks": info.num_blocks, "block_size": info.block_size}),
    )
}

fn bdev_lvol_create_lvstore(p: BlobstoreConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    let info = mgr.create_lvol_store(&p)?;
    Ok(serde_json::json!({
        "uuid": info.name,
        "total_clusters": info.total_clusters,
        "free_clusters": info.free_clusters,
    }))
}

fn bdev_lvol_create(p: LvolConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    let info = mgr.create_lvol(&p)?;
    Ok(serde_json::json!({"name": info.name, "num_blocks": info.num_blocks}))
}

#[derive(Deserialize)]
struct BdevDeleteParams {
    name: String,
}

fn bdev_delete(p: BdevDeleteParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    mgr.delete_bdev(&p.name)?;
    Ok(serde_json::json!(true))
}

fn bdev_list(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    let bdevs: Vec<serde_json::Value> = mgr
        .list_bdevs()
        .iter()
        .map(|b| {
            serde_json::json!({
                "name": b.name,
                "device_path": b.device_path,
                "block_size": b.block_size,
                "num_blocks": b.num_blocks,
                "bdev_type": b.bdev_type,
            })
        })
        .collect();
    Ok(serde_json::json!(bdevs))
}

#[derive(Deserialize)]
struct BdevGetInfoParams {
    name: String,
}

fn bdev_get_info(p: BdevGetInfoParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    let info = mgr
        .get_bdev(&p.name)
        .ok_or_else(|| DataPlaneError::BdevError(format!("bdev '{}' not found", p.name)))?;
    Ok(serde_json::json!({
        "name": info.name,
        "device_path": info.device_path,
        "block_size": info.block_size,
        "num_blocks": info.num_blocks,
        "bdev_type": info.bdev_type,
        "size_bytes": info.num_blocks as u64 * info.block_size as u64,
    }))
}

fn bdev_lvol_list_lvstores(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = BDEV_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "bdev manager not initialised".into(),
    ))?;
    let stores: Vec<serde_json::Value> = mgr
        .list_lvol_stores()
        .iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "base_bdev": s.base_bdev,
                "cluster_size": s.cluster_size,
                "total_clusters": s.total_clusters,
                "free_clusters": s.free_clusters,
            })
        })
        .collect();
    Ok(serde_json::json!(stores))
}

// ---------------------------------------------------------------------------
// NVMe-oF methods
// ---------------------------------------------------------------------------

fn nvmf_init_transport(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    mgr.ensure_transport()?;
    Ok(serde_json::json!({"status": "ok"}))
}

fn nvmf_create_target(p: NvmfTargetConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    let info = mgr.create_target(&p)?;
    Ok(serde_json::json!({
        "nqn": info.nqn,
        "bdev_name": info.bdev_name,
        "listen_address": info.listen_address,
        "listen_port": info.listen_port,
        "ana_group_id": info.ana_group_id,
        "ana_state": info.ana_state,
    }))
}

#[derive(Deserialize)]
struct NvmfDeleteTargetParams {
    volume_id: String,
}

fn nvmf_delete_target(p: NvmfDeleteTargetParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    mgr.delete_target(&p.volume_id)?;
    Ok(serde_json::json!(true))
}

fn nvmf_connect_initiator(p: NvmfInitiatorConfig) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    let info = mgr.connect_initiator(&p)?;
    Ok(serde_json::json!({
        "bdev_name": info.local_bdev_name,
        "nqn": info.nqn,
        "remote_address": info.remote_address,
        "remote_port": info.remote_port,
    }))
}

#[derive(Deserialize)]
struct NvmfDisconnectParams {
    bdev_name: String,
}

fn nvmf_disconnect_initiator(p: NvmfDisconnectParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    mgr.disconnect_initiator(&p.bdev_name)?;
    Ok(serde_json::json!(true))
}

#[derive(Deserialize)]
struct NvmfExportLocalParams {
    bdev_name: String,
    volume_id: String,
}

fn nvmf_export_local(p: NvmfExportLocalParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    let info = mgr.export_local(&p.bdev_name, &p.volume_id)?;
    Ok(serde_json::json!({
        "nqn": info.nqn,
        "listen_address": info.listen_address,
        "listen_port": info.listen_port,
    }))
}

#[derive(Deserialize)]
struct SetAnaStateParams {
    nqn: String,
    ana_group_id: u32,
    ana_state: String,
}

fn nvmf_set_ana_state(p: SetAnaStateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    mgr.set_ana_state(&p.nqn, p.ana_group_id, &p.ana_state)?;
    Ok(serde_json::json!({}))
}

#[derive(Deserialize)]
struct GetAnaStateParams {
    nqn: String,
}

fn nvmf_get_ana_state(p: GetAnaStateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    let (ana_group_id, ana_state) = mgr.get_ana_state(&p.nqn)?;
    Ok(serde_json::json!({
        "ana_group_id": ana_group_id,
        "ana_state": ana_state,
    }))
}

fn nvmf_list_subsystems(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    let subsystems: Vec<serde_json::Value> = mgr
        .list_subsystems()
        .iter()
        .map(|s| {
            serde_json::json!({
                "nqn": s.nqn,
                "bdev_name": s.bdev_name,
                "listen_address": s.listen_address,
                "listen_port": s.listen_port,
                "ana_group_id": s.ana_group_id,
                "ana_state": s.ana_state,
            })
        })
        .collect();
    Ok(serde_json::json!(subsystems))
}

fn nvmf_list_initiators(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    let initiators: Vec<serde_json::Value> = mgr
        .list_initiators()
        .iter()
        .map(|i| {
            serde_json::json!({
                "nqn": i.nqn,
                "remote_address": i.remote_address,
                "remote_port": i.remote_port,
                "local_bdev_name": i.local_bdev_name,
            })
        })
        .collect();
    Ok(serde_json::json!(initiators))
}

#[derive(Deserialize)]
struct NvmfGetSubsystemParams {
    nqn: String,
}

fn nvmf_get_subsystem(p: NvmfGetSubsystemParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or(DataPlaneError::SpdkInit(
        "nvmf manager not initialised".into(),
    ))?;
    let info = mgr.get_subsystem(&p.nqn).ok_or_else(|| {
        DataPlaneError::NvmfTargetError(format!("subsystem '{}' not found", p.nqn))
    })?;
    Ok(serde_json::json!({
        "nqn": info.nqn,
        "bdev_name": info.bdev_name,
        "listen_address": info.listen_address,
        "listen_port": info.listen_port,
        "ana_group_id": info.ana_group_id,
        "ana_state": info.ana_state,
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
    /// Size of the replica bdev in bytes, required for SPDK bdev registration.
    #[serde(default)]
    size_bytes: u64,
}

#[derive(Deserialize)]
struct ReplicaTargetParam {
    addr: String,
    port: u16,
    nqn: String,
    #[serde(default)]
    is_local: bool,
    /// Explicit bdev name for this target (for local malloc bdev testing).
    #[serde(default)]
    bdev_name: Option<String>,
}

fn replica_bdev_create(p: ReplicaBdevCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    let read_policy = match p.read_policy.as_deref() {
        Some("local_first") => {
            let local_addr = p
                .targets
                .iter()
                .find(|t| t.is_local)
                .map(|t| t.addr.clone())
                .unwrap_or_default();
            ReadPolicy::LocalFirst {
                local_address: local_addr,
            }
        }
        Some("latency_aware") => ReadPolicy::LatencyAware,
        _ => ReadPolicy::RoundRobin,
    };

    let replicas: Vec<ReplicaTarget> = p
        .targets
        .iter()
        .map(|t| ReplicaTarget {
            address: t.addr.clone(),
            port: t.port,
            nqn: t.nqn.clone(),
            // Use explicit bdev_name if provided; otherwise fall back to the
            // nqn field which the Go agent sets to the actual bdev name
            // (chunk bdev name for local, initiator bdev name for remote).
            bdev_name: t.bdev_name.clone().or_else(|| Some(t.nqn.clone())),
        })
        .collect();

    let replica_count = replicas.len();
    let write_quorum = (replica_count as u32 / 2) + 1;

    let config = ReplicaBdevConfig {
        volume_id: p.name.clone(),
        replicas,
        write_quorum,
        read_policy,
    };

    let mut bdev = ReplicaBdev::new(config);
    bdev.enable_spdk_io();
    let bdev = Arc::new(bdev);
    let bdev_name = bdev.bdev_name.clone();

    // Register the replica bdev with SPDK so it can be used as an NVMe-oF
    // namespace. This is required for NVMe-oF export of replicated volumes.
    let spdk_bdev_name = if p.size_bytes > 0 {
        novastor_replica_bdev::create(bdev.clone(), p.size_bytes)?
    } else {
        bdev_name.clone()
    };

    let mut registry = replica_bdevs().lock().unwrap();
    registry.insert(p.name.clone(), bdev);

    Ok(serde_json::json!({
        "name": spdk_bdev_name,
        "replica_count": replica_count,
        "write_quorum": write_quorum,
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
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let target = ReplicaTarget {
        address: p.target.addr,
        port: p.target.port,
        nqn: p.target.nqn,
        bdev_name: None,
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
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
        })?
        .clone();
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
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let status = bdev.full_status();
    let replica_infos: Vec<serde_json::Value> = status
        .replicas
        .iter()
        .map(|r| {
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
        })
        .collect();

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
// Replica bdev I/O methods
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ReplicaWriteParams {
    volume_id: String,
    offset: u64,
    data_base64: String,
}

fn handle_replica_write(p: ReplicaWriteParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let registry = replica_bdevs().lock().unwrap();
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let data = base64::engine::general_purpose::STANDARD
        .decode(&p.data_base64)
        .map_err(|e| DataPlaneError::ReplicaError(format!("invalid base64: {e}")))?;

    bdev.submit_write(p.offset, &data)?;
    Ok(serde_json::json!({"bytes_written": data.len()}))
}

#[derive(Deserialize)]
struct ReplicaReadParams {
    volume_id: String,
    offset: u64,
    length: u64,
}

fn handle_replica_read(p: ReplicaReadParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let registry = replica_bdevs().lock().unwrap();
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let data = bdev.submit_read(p.offset, p.length)?;
    Ok(serde_json::json!({
        "data_base64": base64::engine::general_purpose::STANDARD.encode(&data),
        "length": data.len(),
    }))
}

// ---------------------------------------------------------------------------
// Erasure coding bdev methods
// ---------------------------------------------------------------------------

fn erasure_bdev_create(p: ErasureBdevConfig) -> Result<serde_json::Value, DataPlaneError> {
    let volume_id = p.volume_id.clone();
    let mut bdev = ErasureBdev::new(p)?;
    bdev.enable_spdk_io();
    let bdev = Arc::new(bdev);
    let bdev_name = bdev.bdev_name.clone();
    let status = bdev.status();

    let mut registry = erasure_bdevs().lock().unwrap();
    registry.insert(volume_id.clone(), bdev);

    Ok(serde_json::json!({
        "name": bdev_name,
        "volume_id": volume_id,
        "data_shards": status.data_shards,
        "parity_shards": status.parity_shards,
    }))
}

#[derive(Deserialize)]
struct ErasureStatusParams {
    volume_id: String,
}

fn erasure_bdev_status(p: ErasureStatusParams) -> Result<serde_json::Value, DataPlaneError> {
    let registry = erasure_bdevs().lock().unwrap();
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ErasureError(format!("erasure bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let status = bdev.status();
    Ok(serde_json::to_value(status)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

#[derive(Deserialize)]
struct ErasureEncodeParams {
    volume_id: String,
    /// Base64-encoded data to encode.
    data_base64: String,
}

fn erasure_encode(p: ErasureEncodeParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let registry = erasure_bdevs().lock().unwrap();
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ErasureError(format!("erasure bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let data = base64::engine::general_purpose::STANDARD
        .decode(&p.data_base64)
        .map_err(|e| DataPlaneError::ErasureError(format!("invalid base64: {e}")))?;

    let shards = bdev.encode(&data)?;
    let shard_b64: Vec<String> = shards
        .iter()
        .map(|s| base64::engine::general_purpose::STANDARD.encode(s))
        .collect();

    Ok(serde_json::json!({
        "shards": shard_b64,
        "original_size": data.len(),
        "shard_size": shards[0].len(),
    }))
}

#[derive(Deserialize)]
struct ErasureDecodeParams {
    volume_id: String,
    /// Map of shard_index → base64-encoded shard data.
    available_shards: HashMap<String, String>,
    original_size: usize,
}

fn erasure_decode(p: ErasureDecodeParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let registry = erasure_bdevs().lock().unwrap();
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ErasureError(format!("erasure bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let mut available: Vec<(usize, Vec<u8>)> = Vec::new();
    for (idx_str, b64) in &p.available_shards {
        let idx: usize = idx_str.parse().map_err(|e| {
            DataPlaneError::ErasureError(format!("invalid shard index '{}': {}", idx_str, e))
        })?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| {
                DataPlaneError::ErasureError(format!("invalid base64 for shard {}: {e}", idx))
            })?;
        available.push((idx, data));
    }

    let recovered = bdev.decode(&available, p.original_size)?;

    Ok(serde_json::json!({
        "data_base64": base64::engine::general_purpose::STANDARD.encode(&recovered),
        "size": recovered.len(),
    }))
}

// ---------------------------------------------------------------------------
// Erasure coding bdev I/O methods
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ErasureWriteParams {
    volume_id: String,
    offset: u64,
    data_base64: String,
}

fn handle_erasure_write(p: ErasureWriteParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let registry = erasure_bdevs().lock().unwrap();
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ErasureError(format!("erasure bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let data = base64::engine::general_purpose::STANDARD
        .decode(&p.data_base64)
        .map_err(|e| DataPlaneError::ErasureError(format!("invalid base64: {e}")))?;

    bdev.submit_write(p.offset, &data)?;
    Ok(serde_json::json!({"bytes_written": data.len()}))
}

#[derive(Deserialize)]
struct ErasureReadParams {
    volume_id: String,
    offset: u64,
    original_size: usize,
}

fn handle_erasure_read(p: ErasureReadParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let registry = erasure_bdevs().lock().unwrap();
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ErasureError(format!("erasure bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let data = bdev.submit_read(p.offset, p.original_size)?;
    Ok(serde_json::json!({
        "data_base64": base64::engine::general_purpose::STANDARD.encode(&data),
        "size": data.len(),
    }))
}

// ---------------------------------------------------------------------------
// Chunk I/O methods
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ChunkStoreCreateParams {
    bdev_name: String,
    capacity_bytes: u64,
}

fn chunk_store_create(p: ChunkStoreCreateParams) -> Result<serde_json::Value, DataPlaneError> {
    let store = Arc::new(ChunkStore::new(&p.bdev_name, p.capacity_bytes));
    let mut stores = chunk_stores().lock().unwrap();
    stores.insert(p.bdev_name.clone(), store);
    Ok(serde_json::json!({"bdev_name": p.bdev_name, "capacity_bytes": p.capacity_bytes}))
}

#[derive(Deserialize)]
struct ChunkWriteParams {
    bdev_name: String,
    /// Base64-encoded chunk data.
    data_base64: String,
}

fn chunk_write(p: ChunkWriteParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let data = base64::engine::general_purpose::STANDARD
        .decode(&p.data_base64)
        .map_err(|e| DataPlaneError::BdevError(format!("invalid base64: {e}")))?;

    let chunk_id = store.write_chunk(&data)?;
    let checksum = ChunkStore::compute_checksum(&data);

    Ok(serde_json::json!({
        "chunk_id": chunk_id,
        "size": data.len(),
        "checksum": checksum,
    }))
}

#[derive(Deserialize)]
struct ChunkReadParams {
    bdev_name: String,
    chunk_id: String,
}

fn chunk_read(p: ChunkReadParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let data = store.read_chunk(&p.chunk_id)?;
    let meta = store.get_chunk_meta(&p.chunk_id);

    Ok(serde_json::json!({
        "chunk_id": p.chunk_id,
        "data_base64": base64::engine::general_purpose::STANDARD.encode(&data),
        "size": data.len(),
        "checksum": meta.map(|m| m.checksum).unwrap_or(0),
    }))
}

#[derive(Deserialize)]
struct ChunkDeleteParams {
    bdev_name: String,
    chunk_id: String,
}

fn chunk_delete(p: ChunkDeleteParams) -> Result<serde_json::Value, DataPlaneError> {
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    store.delete_chunk(&p.chunk_id)?;
    Ok(serde_json::json!(true))
}

#[derive(Deserialize)]
struct ChunkExistsParams {
    bdev_name: String,
    chunk_id: String,
}

fn chunk_exists(p: ChunkExistsParams) -> Result<serde_json::Value, DataPlaneError> {
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    Ok(serde_json::json!({"exists": store.has_chunk(&p.chunk_id)}))
}

#[derive(Deserialize)]
struct ChunkListParams {
    bdev_name: String,
}

fn chunk_list(p: ChunkListParams) -> Result<serde_json::Value, DataPlaneError> {
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let chunks = store.list_chunks();
    Ok(serde_json::json!(chunks))
}

fn chunk_store_stats(p: ChunkListParams) -> Result<serde_json::Value, DataPlaneError> {
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let stats = store.stats();
    Ok(serde_json::to_value(stats)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

fn chunk_scrub(p: ChunkListParams) -> Result<serde_json::Value, DataPlaneError> {
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let result = store.scrub()?;
    Ok(serde_json::to_value(result)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

#[derive(Deserialize)]
struct ChunkSplitWriteParams {
    bdev_name: String,
    data_base64: String,
}

fn chunk_split_write(p: ChunkSplitWriteParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let data = base64::engine::general_purpose::STANDARD
        .decode(&p.data_base64)
        .map_err(|e| DataPlaneError::BdevError(format!("invalid base64: {e}")))?;

    let chunks = store.split_and_write(&data)?;
    let chunk_list: Vec<serde_json::Value> = chunks
        .iter()
        .map(|(id, size)| serde_json::json!({"chunk_id": id, "size": size}))
        .collect();

    Ok(serde_json::json!({
        "chunks": chunk_list,
        "total_size": data.len(),
    }))
}

#[derive(Deserialize)]
struct ChunkReassembleParams {
    bdev_name: String,
    chunk_ids: Vec<String>,
}

fn chunk_reassemble(p: ChunkReassembleParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let data = store.reassemble(&p.chunk_ids)?;

    Ok(serde_json::json!({
        "data_base64": base64::engine::general_purpose::STANDARD.encode(&data),
        "size": data.len(),
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
    let bdev = registry
        .get(&p.volume_id)
        .ok_or_else(|| {
            DataPlaneError::ReplicaError(format!("replica bdev {} not found", p.volume_id))
        })?
        .clone();
    drop(registry);

    let stats = bdev.io_stats();
    let replica_stats: Vec<serde_json::Value> = stats
        .replicas
        .iter()
        .map(|r| {
            serde_json::json!({
                "addr": r.address,
                "port": r.port,
                "reads_completed": r.reads_completed,
                "read_bytes": r.read_bytes,
                "avg_read_latency_us": r.avg_read_latency_us,
            })
        })
        .collect();

    Ok(serde_json::json!({
        "volume_id": stats.volume_id,
        "replicas": replica_stats,
        "total_read_iops": stats.total_read_iops,
        "total_write_iops": stats.total_write_iops,
        "write_quorum_latency_us": stats.write_quorum_latency_us,
    }))
}

// ---------------------------------------------------------------------------
// Unified storage backend methods
// ---------------------------------------------------------------------------

fn backend_init_raw_disk(_p: serde_json::Value) -> Result<serde_json::Value, DataPlaneError> {
    let backend = Arc::new(RawDiskBackend::new());
    backends()
        .lock()
        .unwrap()
        .insert("raw_disk".to_string(), backend);
    Ok(serde_json::json!({"backend": "raw_disk", "status": "initialised"}))
}

#[derive(Deserialize)]
struct BackendInitLvmParams {
    lvol_store: String,
}

fn backend_init_lvm(p: BackendInitLvmParams) -> Result<serde_json::Value, DataPlaneError> {
    let backend = Arc::new(LvmBackend::new(&p.lvol_store));
    backends()
        .lock()
        .unwrap()
        .insert("lvm".to_string(), backend);
    Ok(serde_json::json!({"backend": "lvm", "lvol_store": p.lvol_store, "status": "initialised"}))
}

#[derive(Deserialize)]
struct BackendInitChunkParams {
    bdev_name: String,
    capacity_bytes: u64,
}

fn backend_init_chunk(p: BackendInitChunkParams) -> Result<serde_json::Value, DataPlaneError> {
    let store = Arc::new(ChunkStore::new(&p.bdev_name, p.capacity_bytes));
    let backend: Arc<dyn StorageBackend> = Arc::new(ChunkBackend::new(store));

    // Register the chunk backend with the novastor_bdev module so it can
    // bridge SPDK block I/O to ChunkBackend::read/write.
    novastor_bdev::set_chunk_backend(backend.clone());

    backends()
        .lock()
        .unwrap()
        .insert("chunk".to_string(), backend);
    Ok(serde_json::json!({"backend": "chunk", "bdev_name": p.bdev_name, "status": "initialised"}))
}

#[derive(Deserialize)]
struct BackendCreateVolumeParams {
    backend: String,
    name: String,
    size_bytes: u64,
    #[serde(default)]
    thin: bool,
}

fn backend_create_volume(
    p: BackendCreateVolumeParams,
) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let info = backend.create_volume(&p.name, p.size_bytes, p.thin)?;

    // For chunk backend volumes, automatically register an SPDK bdev so
    // the volume can be exposed via NVMe-oF without a separate RPC call.
    let mut result = serde_json::to_value(&info)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?;

    if p.backend == "chunk" {
        let bdev_name = novastor_bdev::create(&p.name, p.size_bytes)?;
        if let Some(obj) = result.as_object_mut() {
            obj.insert(
                "bdev_name".to_string(),
                serde_json::Value::String(bdev_name),
            );
        }
    }

    Ok(result)
}

#[derive(Deserialize)]
struct BackendVolumeNameParams {
    backend: String,
    name: String,
}

fn backend_delete_volume(p: BackendVolumeNameParams) -> Result<serde_json::Value, DataPlaneError> {
    // For chunk backend, destroy the SPDK bdev first (before deleting the
    // volume data) so that any NVMe-oF subsystem referencing it is cleaned up.
    if p.backend == "chunk" {
        // Best-effort: bdev may not exist if creation failed partway.
        let _ = novastor_bdev::destroy(&p.name);
    }

    let backend = get_backend(&p.backend)?;
    backend.delete_volume(&p.name)?;
    Ok(serde_json::json!(true))
}

#[derive(Deserialize)]
struct BackendResizeParams {
    backend: String,
    name: String,
    new_size_bytes: u64,
}

fn backend_resize_volume(p: BackendResizeParams) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let info = backend.resize_volume(&p.name, p.new_size_bytes)?;
    Ok(serde_json::to_value(info)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

fn backend_stat_volume(p: BackendVolumeNameParams) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let info = backend.stat_volume(&p.name)?;
    Ok(serde_json::to_value(info)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

#[derive(Deserialize)]
struct BackendTypeParams {
    backend: String,
}

fn backend_list_volumes(p: BackendTypeParams) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let volumes = backend.list_volumes()?;
    Ok(serde_json::to_value(volumes)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

#[derive(Deserialize)]
struct BackendReadParams {
    backend: String,
    name: String,
    offset: u64,
    length: u64,
}

fn backend_read(p: BackendReadParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let backend = get_backend(&p.backend)?;
    let data = backend.read(&p.name, p.offset, p.length)?;
    Ok(serde_json::json!({
        "data_base64": base64::engine::general_purpose::STANDARD.encode(&data),
        "length": data.len(),
    }))
}

#[derive(Deserialize)]
struct BackendWriteParams {
    backend: String,
    name: String,
    offset: u64,
    data_base64: String,
}

fn backend_write(p: BackendWriteParams) -> Result<serde_json::Value, DataPlaneError> {
    use base64::Engine;
    let backend = get_backend(&p.backend)?;
    let data = base64::engine::general_purpose::STANDARD
        .decode(&p.data_base64)
        .map_err(|e| DataPlaneError::BdevError(format!("invalid base64: {e}")))?;
    backend.write(&p.name, p.offset, &data)?;
    Ok(serde_json::json!({"bytes_written": data.len()}))
}

#[derive(Deserialize)]
struct BackendSnapshotParams {
    backend: String,
    volume_name: String,
    snapshot_name: String,
}

fn backend_create_snapshot(p: BackendSnapshotParams) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let info = backend.create_snapshot(&p.volume_name, &p.snapshot_name)?;
    Ok(serde_json::to_value(info)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

#[derive(Deserialize)]
struct BackendDeleteSnapshotParams {
    backend: String,
    snapshot_name: String,
}

fn backend_delete_snapshot(
    p: BackendDeleteSnapshotParams,
) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    backend.delete_snapshot(&p.snapshot_name)?;
    Ok(serde_json::json!(true))
}

fn backend_list_snapshots(p: BackendVolumeNameParams) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let snapshots = backend.list_snapshots(&p.name)?;
    Ok(serde_json::to_value(snapshots)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
}

#[derive(Deserialize)]
struct BackendCloneParams {
    backend: String,
    snapshot_name: String,
    clone_name: String,
}

fn backend_clone(p: BackendCloneParams) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let info = StorageBackend::clone(&*backend, &p.snapshot_name, &p.clone_name)?;
    Ok(serde_json::to_value(info)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?)
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

// ---------------------------------------------------------------------------
// Chunk garbage collection
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ChunkGcParams {
    bdev_name: String,
    /// Chunk IDs that are still in use and should NOT be deleted.
    valid_chunk_ids: Vec<String>,
}

fn chunk_gc(p: ChunkGcParams) -> Result<serde_json::Value, DataPlaneError> {
    let stores = chunk_stores().lock().unwrap();
    let store = stores
        .get(&p.bdev_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("chunk store '{}' not found", p.bdev_name))
        })?
        .clone();
    drop(stores);

    let all_chunks = store.list_chunks();
    let valid_set: std::collections::HashSet<&str> =
        p.valid_chunk_ids.iter().map(|s| s.as_str()).collect();

    let mut deleted = 0u64;
    let mut errors = 0u64;
    for chunk_id in &all_chunks {
        if !valid_set.contains(chunk_id.as_str()) {
            match store.delete_chunk(chunk_id) {
                Ok(()) => deleted += 1,
                Err(e) => {
                    log::warn!(
                        "chunk_gc: failed to delete orphan chunk {}: {}",
                        chunk_id,
                        e
                    );
                    errors += 1;
                }
            }
        }
    }

    Ok(serde_json::json!({
        "total_chunks": all_chunks.len(),
        "valid_chunks": valid_set.len(),
        "deleted": deleted,
        "errors": errors,
    }))
}
