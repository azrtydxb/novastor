//! gRPC DataplaneService — the sole communication channel between the Go
//! management agent and the Rust SPDK dataplane (invariant #5).
//!
//! Each method delegates to SPDK manager code. This is a thin adapter that
//! translates gRPC request/response types to the internal API.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::transport::dataplane_proto::dataplane_service_server::DataplaneService;
use crate::transport::dataplane_proto::*;

use crate::backend::chunk_store::ChunkStore as AsyncChunkStore;
use crate::backend::traits::StorageBackend;
use crate::bdev::chunk_io::{ChunkStore, CHUNK_SIZE};
use crate::config::{BlobstoreConfig, LocalBdevConfig, LvolConfig, NvmfInitiatorConfig};
use crate::policy::engine::PolicyEngine;
use crate::spdk::bdev_manager::BdevManager;
use crate::spdk::nvmf_manager::NvmfManager;

/// Fragment size for streaming chunk data (1MB).
const STREAM_FRAGMENT_SIZE: usize = 1024 * 1024;

/// Fencing timeout in seconds. If no heartbeat is received within this
/// duration, the dataplane enters fenced mode and rejects writes.
const FENCING_TIMEOUT_SECS: i64 = 5;

/// Global last heartbeat timestamp (Unix seconds).
static LAST_HEARTBEAT: AtomicI64 = AtomicI64::new(0);

/// Check if the dataplane is currently fenced (no recent heartbeat).
pub fn is_fenced() -> bool {
    let last = LAST_HEARTBEAT.load(Ordering::Relaxed);
    if last == 0 {
        // No heartbeat ever received — not fenced (startup grace period).
        return false;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    now - last > FENCING_TIMEOUT_SECS
}

/// Global registry of chunk stores, keyed by bdev name.
static GRPC_CHUNK_STORES: OnceLock<Mutex<std::collections::HashMap<String, Arc<ChunkStore>>>> =
    OnceLock::new();

fn grpc_chunk_stores() -> &'static Mutex<std::collections::HashMap<String, Arc<ChunkStore>>> {
    GRPC_CHUNK_STORES.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Global registry of async chunk stores for inter-dataplane chunk transfer,
/// keyed by bdev name. These implement the async `ChunkStore` trait (put/get)
/// and are used by the PutChunk/GetChunk RPCs during replication.
static GRPC_ASYNC_CHUNK_STORES: OnceLock<
    Mutex<std::collections::HashMap<String, Arc<dyn AsyncChunkStore>>>,
> = OnceLock::new();

fn grpc_async_chunk_stores(
) -> &'static Mutex<std::collections::HashMap<String, Arc<dyn AsyncChunkStore>>> {
    GRPC_ASYNC_CHUNK_STORES.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Register an async chunk store for inter-dataplane PutChunk/GetChunk RPCs.
/// Called by `init_chunk_store` in production and by tests.
pub fn register_async_chunk_store(bdev_name: &str, store: Arc<dyn AsyncChunkStore>) {
    grpc_async_chunk_stores()
        .lock()
        .unwrap()
        .insert(bdev_name.to_string(), store);
}

/// Return the first available async chunk store. Inter-dataplane PutChunk/GetChunk
/// RPCs use this when no bdev name is specified (single-store deployments).
fn get_any_async_chunk_store() -> Result<Arc<dyn AsyncChunkStore>, Status> {
    let stores = grpc_async_chunk_stores().lock().unwrap();
    stores.values().next().cloned().ok_or_else(|| {
        Status::failed_precondition("no async chunk store initialised — call InitChunkStore first")
    })
}

/// Global registry of storage backends, keyed by a label (e.g., pool name or backend type).
static GRPC_BACKENDS: OnceLock<Mutex<std::collections::HashMap<String, Arc<dyn StorageBackend>>>> =
    OnceLock::new();

fn grpc_backends() -> &'static Mutex<std::collections::HashMap<String, Arc<dyn StorageBackend>>> {
    GRPC_BACKENDS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Global registry of policy engines, keyed by bdev name.
static GRPC_POLICY_ENGINES: OnceLock<Mutex<std::collections::HashMap<String, Arc<PolicyEngine>>>> =
    OnceLock::new();

fn grpc_policy_engines() -> &'static Mutex<std::collections::HashMap<String, Arc<PolicyEngine>>> {
    GRPC_POLICY_ENGINES.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn get_policy_engine(bdev_name: &str) -> Result<Arc<PolicyEngine>, Status> {
    let engines = grpc_policy_engines().lock().unwrap();
    engines.get(bdev_name).cloned().ok_or_else(|| {
        Status::not_found(format!("policy engine for '{}' not initialised", bdev_name))
    })
}

/// Return the first available policy engine (for RPCs that don't specify a bdev).
fn get_any_policy_engine() -> Result<Arc<PolicyEngine>, Status> {
    let engines = grpc_policy_engines().lock().unwrap();
    engines.values().next().cloned().ok_or_else(|| {
        Status::failed_precondition("no policy engine initialised — call InitChunkStore first")
    })
}

fn get_backend(key: &str) -> Result<Arc<dyn StorageBackend>, Status> {
    let backends = grpc_backends().lock().unwrap();
    backends
        .get(key)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("backend '{}' not initialised", key)))
}

fn get_chunk_store(bdev_name: &str) -> Result<Arc<ChunkStore>, Status> {
    let stores = grpc_chunk_stores().lock().unwrap();
    stores
        .get(bdev_name)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("chunk store '{}' not initialised", bdev_name)))
}

/// Convert a volume ID to an NQN string.
fn volume_id_to_nqn(volume_id: &str) -> String {
    format!("nqn.2024-01.io.novastor:volume-{}", volume_id)
}

/// The gRPC DataplaneService implementation.
pub struct DataplaneServiceImpl {
    bdev_manager: Arc<BdevManager>,
    nvmf_manager: Arc<NvmfManager>,
}

impl DataplaneServiceImpl {
    pub fn new(bdev_manager: Arc<BdevManager>, nvmf_manager: Arc<NvmfManager>) -> Self {
        Self {
            bdev_manager,
            nvmf_manager,
        }
    }
}

type StreamResult<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl DataplaneService for DataplaneServiceImpl {
    // ========================================================================
    // Bdev Management
    // ========================================================================

    async fn create_aio_bdev(
        &self,
        request: Request<CreateAioBdevRequest>,
    ) -> Result<Response<BdevInfo>, Status> {
        let req = request.into_inner();
        let config = LocalBdevConfig {
            name: req.name,
            device_path: req.device_path,
            block_size: req.block_size,
        };
        let info = self
            .bdev_manager
            .create_aio_bdev(&config)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(BdevInfo {
            name: info.name,
            device_path: info.device_path,
            block_size: info.block_size,
            num_blocks: info.num_blocks,
            bdev_type: info.bdev_type,
            size_bytes: info.num_blocks * info.block_size as u64,
        }))
    }

    async fn create_malloc_bdev(
        &self,
        request: Request<CreateMallocBdevRequest>,
    ) -> Result<Response<BdevInfo>, Status> {
        let req = request.into_inner();
        let info = self
            .bdev_manager
            .create_malloc_bdev(&req.name, req.size_mb, req.block_size)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(BdevInfo {
            name: info.name,
            device_path: info.device_path,
            block_size: info.block_size,
            num_blocks: info.num_blocks,
            bdev_type: info.bdev_type,
            size_bytes: info.num_blocks * info.block_size as u64,
        }))
    }

    async fn create_lvol_store(
        &self,
        request: Request<CreateLvolStoreRequest>,
    ) -> Result<Response<LvolStoreInfo>, Status> {
        let req = request.into_inner();
        let config = BlobstoreConfig {
            base_bdev: req.bdev_name,
            cluster_size: if req.cluster_size > 0 {
                req.cluster_size
            } else {
                1024 * 1024 // default 1MB
            },
        };
        let info = self
            .bdev_manager
            .create_lvol_store(&config)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(LvolStoreInfo {
            uuid: info.name.clone(),
            name: info.name,
            base_bdev: info.base_bdev,
            cluster_size: info.cluster_size,
            total_clusters: info.total_clusters,
            free_clusters: info.free_clusters,
        }))
    }

    async fn create_lvol(
        &self,
        request: Request<CreateLvolRequest>,
    ) -> Result<Response<BdevInfo>, Status> {
        let req = request.into_inner();
        let config = LvolConfig {
            volume_id: req.lvol_name,
            size_bytes: req.size_bytes,
            lvol_store: req.lvs_name,
            thin_provision: req.thin_provision,
        };
        let info = self
            .bdev_manager
            .create_lvol(&config)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(BdevInfo {
            name: info.name,
            device_path: info.device_path,
            block_size: info.block_size,
            num_blocks: info.num_blocks,
            bdev_type: info.bdev_type,
            size_bytes: info.num_blocks * info.block_size as u64,
        }))
    }

    async fn delete_bdev(
        &self,
        request: Request<DeleteBdevRequest>,
    ) -> Result<Response<DeleteBdevResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        self.bdev_manager
            .delete_bdev(&req.name)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteBdevResponse {}))
    }

    async fn list_bdevs(
        &self,
        _request: Request<ListBdevsRequest>,
    ) -> Result<Response<ListBdevsResponse>, Status> {
        let bdevs = self
            .bdev_manager
            .list_bdevs()
            .iter()
            .map(|b| BdevInfo {
                name: b.name.clone(),
                device_path: b.device_path.clone(),
                block_size: b.block_size,
                num_blocks: b.num_blocks,
                bdev_type: b.bdev_type.clone(),
                size_bytes: b.num_blocks * b.block_size as u64,
            })
            .collect();
        Ok(Response::new(ListBdevsResponse { bdevs }))
    }

    async fn get_bdev_info(
        &self,
        request: Request<GetBdevInfoRequest>,
    ) -> Result<Response<BdevInfo>, Status> {
        let req = request.into_inner();
        let info = self
            .bdev_manager
            .get_bdev(&req.name)
            .ok_or_else(|| Status::not_found(format!("bdev '{}' not found", req.name)))?;
        Ok(Response::new(BdevInfo {
            name: info.name,
            device_path: info.device_path,
            block_size: info.block_size,
            num_blocks: info.num_blocks,
            bdev_type: info.bdev_type,
            size_bytes: info.num_blocks * info.block_size as u64,
        }))
    }

    async fn list_lvol_stores(
        &self,
        _request: Request<ListLvolStoresRequest>,
    ) -> Result<Response<ListLvolStoresResponse>, Status> {
        let stores = self
            .bdev_manager
            .list_lvol_stores()
            .iter()
            .map(|s| LvolStoreInfo {
                uuid: s.name.clone(),
                name: s.name.clone(),
                base_bdev: s.base_bdev.clone(),
                cluster_size: s.cluster_size,
                total_clusters: s.total_clusters,
                free_clusters: s.free_clusters,
            })
            .collect();
        Ok(Response::new(ListLvolStoresResponse { stores }))
    }

    // ========================================================================
    // NVMe-oF Target Management
    // ========================================================================

    async fn init_transport(
        &self,
        _request: Request<InitTransportRequest>,
    ) -> Result<Response<InitTransportResponse>, Status> {
        self.nvmf_manager
            .ensure_transport()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(InitTransportResponse {}))
    }

    async fn create_nvmf_target(
        &self,
        request: Request<CreateNvmfTargetRequest>,
    ) -> Result<Response<NvmfTargetInfo>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        let config = crate::config::NvmfTargetConfig {
            volume_id: req.volume_id,
            bdev_name: req.bdev_name,
            listen_address: req.listen_address,
            listen_port: req.listen_port as u16,
            ana_state: if req.ana_state.is_empty() {
                "optimized".to_string()
            } else {
                req.ana_state
            },
            ana_group_id: req.ana_group_id,
        };
        let info = self
            .nvmf_manager
            .create_target(&config)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(NvmfTargetInfo {
            nqn: info.nqn,
            bdev_name: info.bdev_name,
            listen_address: info.listen_address,
            listen_port: info.listen_port as u32,
            ana_group_id: info.ana_group_id,
            ana_state: info.ana_state,
        }))
    }

    async fn delete_nvmf_target(
        &self,
        request: Request<DeleteNvmfTargetRequest>,
    ) -> Result<Response<DeleteNvmfTargetResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        self.nvmf_manager
            .delete_target(&req.volume_id)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteNvmfTargetResponse {}))
    }

    async fn set_ana_state(
        &self,
        request: Request<SetAnaStateRequest>,
    ) -> Result<Response<SetAnaStateResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        let nqn = volume_id_to_nqn(&req.volume_id);
        self.nvmf_manager
            .set_ana_state(&nqn, req.ana_group_id, &req.ana_state)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(SetAnaStateResponse {}))
    }

    async fn get_ana_state(
        &self,
        request: Request<GetAnaStateRequest>,
    ) -> Result<Response<GetAnaStateResponse>, Status> {
        let req = request.into_inner();
        let nqn = volume_id_to_nqn(&req.volume_id);
        let (group_id, state) = self
            .nvmf_manager
            .get_ana_state(&nqn)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetAnaStateResponse {
            ana_state: state,
            ana_group_id: group_id,
        }))
    }

    async fn list_subsystems(
        &self,
        _request: Request<ListSubsystemsRequest>,
    ) -> Result<Response<ListSubsystemsResponse>, Status> {
        let subs = self.nvmf_manager.list_subsystems();
        let subsystems = subs
            .iter()
            .map(|s| SubsystemInfo {
                nqn: s.nqn.clone(),
                bdev_name: s.bdev_name.clone(),
                listen_address: s.listen_address.clone(),
                listen_port: s.listen_port as u32,
                ana_state: s.ana_state.clone(),
            })
            .collect();
        Ok(Response::new(ListSubsystemsResponse { subsystems }))
    }

    async fn export_bdev(
        &self,
        _request: Request<ExportBdevRequest>,
    ) -> Result<Response<ExportBdevResponse>, Status> {
        // ExportBdev adds a namespace to an existing subsystem.
        // Currently handled internally during create_target. Exposed as a
        // separate RPC for future use when subsystem and namespace management
        // are decoupled.
        Err(Status::unimplemented(
            "export_bdev is handled internally during create_nvmf_target",
        ))
    }

    // ========================================================================
    // NVMe-oF Initiator
    // ========================================================================

    async fn connect_initiator(
        &self,
        request: Request<ConnectInitiatorRequest>,
    ) -> Result<Response<ConnectInitiatorResponse>, Status> {
        let req = request.into_inner();
        let port: u16 = req
            .port
            .parse()
            .map_err(|e| Status::invalid_argument(format!("invalid port: {e}")))?;
        let config = NvmfInitiatorConfig {
            nqn: req.nqn,
            remote_address: req.address,
            remote_port: port,
            bdev_name: String::new(),
        };
        let info = self
            .nvmf_manager
            .connect_initiator(&config)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ConnectInitiatorResponse {
            bdev_name: info.local_bdev_name,
            nqn: info.nqn,
        }))
    }

    async fn disconnect_initiator(
        &self,
        request: Request<DisconnectInitiatorRequest>,
    ) -> Result<Response<DisconnectInitiatorResponse>, Status> {
        let req = request.into_inner();
        // NvmfManager::disconnect_initiator takes a bdev_name, but the
        // proto passes an NQN. Look up the initiator to find the bdev name.
        let initiators = self.nvmf_manager.list_initiators();
        let initiator = initiators
            .iter()
            .find(|i| i.nqn == req.nqn)
            .ok_or_else(|| {
                Status::not_found(format!("initiator with nqn '{}' not found", req.nqn))
            })?;
        self.nvmf_manager
            .disconnect_initiator(&initiator.local_bdev_name)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DisconnectInitiatorResponse {}))
    }

    // ========================================================================
    // Replica Bdev (stub — will be replaced by chunk engine replication)
    // ========================================================================

    async fn create_replica_bdev(
        &self,
        _request: Request<CreateReplicaBdevRequest>,
    ) -> Result<Response<CreateReplicaBdevResponse>, Status> {
        Err(Status::unimplemented(
            "replica bdev creation via gRPC not yet implemented — use chunk engine replication",
        ))
    }

    async fn replica_status(
        &self,
        _request: Request<ReplicaStatusRequest>,
    ) -> Result<Response<ReplicaStatusResponse>, Status> {
        Err(Status::unimplemented(
            "replica status via gRPC not yet implemented",
        ))
    }

    // ========================================================================
    // Chunk Store
    // ========================================================================

    async fn init_chunk_store(
        &self,
        request: Request<InitChunkStoreRequest>,
    ) -> Result<Response<InitChunkStoreResponse>, Status> {
        let req = request.into_inner();

        // If chunk store already exists for this bdev, return its stats.
        if let Some(existing) = grpc_chunk_stores()
            .lock()
            .unwrap()
            .get(&req.bdev_name)
            .cloned()
        {
            let stats = existing.stats();
            let total_slots = stats.capacity_bytes / CHUNK_SIZE as u64;
            log::info!(
                "init_chunk_store: '{}' already initialised, returning stats",
                req.bdev_name
            );
            return Ok(Response::new(InitChunkStoreResponse {
                total_slots,
                used_slots: stats.chunk_count,
                chunk_size: CHUNK_SIZE as u64,
            }));
        }

        // Look up the bdev to get its capacity for the chunk store.
        let bdev_info = self
            .bdev_manager
            .get_bdev(&req.bdev_name)
            .ok_or_else(|| Status::not_found(format!("bdev '{}' not found", req.bdev_name)))?;
        let capacity_bytes = bdev_info.num_blocks * bdev_info.block_size as u64;
        let store = ChunkStore::new(&req.bdev_name, capacity_bytes);
        let stats = store.stats();
        let total_slots = stats.capacity_bytes / CHUNK_SIZE as u64;
        let store = Arc::new(store);
        grpc_chunk_stores()
            .lock()
            .unwrap()
            .insert(req.bdev_name.clone(), store);

        // Wire up the ChunkEngine with PolicyEngine for the novastor bdev module.
        // Create a BdevChunkStore (async ChunkStore impl) on this bdev, then
        // wrap it in a ChunkEngine with policy tracking via a single-node CRUSH
        // topology. The PolicyEngine is registered globally so SetVolumePolicy
        // RPCs can reach it.
        {
            use crate::backend::bdev_store::BdevChunkStore;
            use crate::chunk::engine::ChunkEngine;
            use crate::metadata::topology::{Backend, BackendType, ClusterMap, Node, NodeStatus};
            use crate::policy::location_store::ChunkLocationStore;

            let chunk_store =
                std::sync::Arc::new(BdevChunkStore::new(&req.bdev_name, capacity_bytes));

            // Register the async chunk store globally so inter-dataplane
            // PutChunk/GetChunk RPCs can store/retrieve replicated chunks.
            register_async_chunk_store(&req.bdev_name, chunk_store.clone());

            // Use the real node ID from the Go agent so it matches the node_id
            // in UpdateTopology, ensuring CRUSH local-placement works correctly.
            // Fall back to a synthetic ID for backward compatibility.
            let node_id = if req.node_id.is_empty() {
                format!("local-{}", req.bdev_name)
            } else {
                req.node_id.clone()
            };
            let mut topology = ClusterMap::new(0);
            topology.add_node(Node {
                id: node_id.clone(),
                address: "127.0.0.1".to_string(),
                port: 9500,
                backends: vec![Backend {
                    id: format!("{}-be", req.bdev_name),
                    node_id: node_id.clone(),
                    capacity_bytes,
                    used_bytes: 0,
                    weight: 100,
                    backend_type: BackendType::Bdev,
                }],
                status: NodeStatus::Online,
            });

            // Create the redb-backed location store for chunk→node mappings.
            let policy_db_dir = format!("/var/lib/novastor/policy");
            std::fs::create_dir_all(&policy_db_dir)
                .map_err(|e| Status::internal(format!("create policy db directory: {e}")))?;
            let policy_db_path = format!("{}/{}.redb", policy_db_dir, req.bdev_name);
            let location_store = Arc::new(
                ChunkLocationStore::open(&policy_db_path)
                    .map_err(|e| Status::internal(format!("open policy location store: {e}")))?,
            );

            // Create the PolicyEngine and register it globally.
            let policy_engine = Arc::new(PolicyEngine::new(
                node_id.clone(),
                location_store,
                chunk_store.clone(),
                topology.clone(),
            ));

            grpc_policy_engines()
                .lock()
                .unwrap()
                .insert(req.bdev_name.clone(), policy_engine.clone());

            // Use ChunkEngine::with_policy so every write records chunk
            // locations and volume references in the policy engine.
            let engine = std::sync::Arc::new(ChunkEngine::with_policy(
                node_id,
                chunk_store,
                topology,
                policy_engine.clone(),
            ));

            let handle = tokio::runtime::Handle::current();
            crate::bdev::novastor_bdev::set_chunk_engine(engine, handle);
            crate::bdev::novastor_bdev::set_backend_bdev_name(&req.bdev_name);

            log::info!(
                "init_chunk_store: backend bdev '{}' ready for sub-block I/O",
                req.bdev_name
            );

            // Spawn a background reconciliation loop that runs every 30 seconds.
            let reconcile_engine = policy_engine;
            let reconcile_bdev = req.bdev_name.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    match reconcile_engine.reconcile().await {
                        Ok(action_count) => {
                            if action_count > 0 {
                                log::info!(
                                    "policy reconcile [{}]: {} corrective action(s) executed",
                                    reconcile_bdev,
                                    action_count,
                                );
                            } else {
                                log::debug!(
                                    "policy reconcile [{}]: all chunks healthy",
                                    reconcile_bdev,
                                );
                            }
                        }
                        Err(e) => {
                            log::warn!("policy reconcile [{}] error: {}", reconcile_bdev, e,);
                        }
                    }
                }
            });

            log::info!(
                "init_chunk_store: ChunkEngine + PolicyEngine initialised for bdev '{}' (reconcile loop started)",
                req.bdev_name,
            );

            // Spawn background sync task for periodic chunk hashing + replication.
            crate::chunk::sync::spawn_sync_task(req.bdev_name.clone());
            log::info!(
                "init_chunk_store: background sync task started for bdev '{}'",
                req.bdev_name
            );

            // Open the persistent metadata store and restore any previously
            // persisted chunk maps so volumes survive dataplane restarts.
            let metadata_dir = format!("/var/lib/novastor/metadata");
            if let Err(e) = std::fs::create_dir_all(&metadata_dir) {
                log::warn!(
                    "init_chunk_store: failed to create metadata directory '{}': {}",
                    metadata_dir,
                    e,
                );
            } else {
                let db_path = format!("{}/{}.redb", metadata_dir, req.bdev_name);
                match crate::metadata::store::MetadataStore::open(&db_path) {
                    Ok(store) => {
                        let store = std::sync::Arc::new(store);
                        crate::bdev::novastor_bdev::set_metadata_store(store);
                        crate::bdev::novastor_bdev::load_chunk_maps_from_store();
                        log::info!(
                            "init_chunk_store: MetadataStore opened at '{}', chunk maps restored",
                            db_path,
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            "init_chunk_store: failed to open metadata store at '{}': {} — \
                             chunk maps will not persist across restarts",
                            db_path,
                            e,
                        );
                    }
                }
            }
        }

        Ok(Response::new(InitChunkStoreResponse {
            total_slots,
            used_slots: stats.chunk_count,
            chunk_size: CHUNK_SIZE as u64,
        }))
    }

    async fn chunk_store_stats(
        &self,
        request: Request<ChunkStoreStatsRequest>,
    ) -> Result<Response<ChunkStoreStatsResponse>, Status> {
        let req = request.into_inner();
        let store = get_chunk_store(&req.bdev_name)?;
        let stats = store.stats();
        let total_slots = stats.capacity_bytes / CHUNK_SIZE as u64;
        Ok(Response::new(ChunkStoreStatsResponse {
            total_slots,
            used_slots: stats.chunk_count,
            free_slots: total_slots - stats.chunk_count,
            chunk_size: CHUNK_SIZE as u64,
        }))
    }

    async fn write_chunk(
        &self,
        request: Request<Streaming<WriteChunkRequest>>,
    ) -> Result<Response<WriteChunkResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no writes accepted",
            ));
        }

        let mut stream = request.into_inner();
        let mut data = Vec::new();
        let mut bdev_name = String::new();

        while let Some(req) = stream.message().await? {
            if bdev_name.is_empty() && !req.bdev_name.is_empty() {
                bdev_name = req.bdev_name;
            }
            data.extend_from_slice(&req.data);
        }

        if bdev_name.is_empty() {
            return Err(Status::invalid_argument("bdev_name is required"));
        }

        let store = get_chunk_store(&bdev_name)?;
        let bytes_len = data.len() as i64;
        let chunk_id = store
            .write_chunk(&data)
            .map_err(|e| Status::internal(format!("write_chunk: {e}")))?;

        Ok(Response::new(WriteChunkResponse {
            chunk_id,
            bytes_written: bytes_len,
        }))
    }

    type ReadChunkStream = StreamResult<ReadChunkResponse>;

    async fn read_chunk(
        &self,
        request: Request<ReadChunkRequest>,
    ) -> Result<Response<Self::ReadChunkStream>, Status> {
        let req = request.into_inner();
        let store = get_chunk_store(&req.bdev_name)?;
        let data = store
            .read_chunk(&req.chunk_id)
            .map_err(|e| Status::internal(format!("read_chunk: {e}")))?;

        let chunk_id = req.chunk_id;
        let stream = async_stream::stream! {
            let mut offset = 0;
            while offset < data.len() {
                let end = std::cmp::min(offset + STREAM_FRAGMENT_SIZE, data.len());
                yield Ok(ReadChunkResponse {
                    chunk_id: if offset == 0 { chunk_id.clone() } else { String::new() },
                    data: data[offset..end].to_vec(),
                });
                offset = end;
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        if req.bdev_name.is_empty() {
            // Inter-dataplane path: no bdev_name specified, use async chunk store.
            let store = get_any_async_chunk_store()?;
            store
                .delete(&req.chunk_id)
                .await
                .map_err(|e| Status::internal(format!("delete_chunk: {e}")))?;
        } else {
            // Go agent path: bdev_name specified, use sync chunk store.
            let store = get_chunk_store(&req.bdev_name)?;
            store
                .delete_chunk(&req.chunk_id)
                .map_err(|e| Status::internal(format!("delete_chunk: {e}")))?;
        }
        Ok(Response::new(DeleteChunkResponse {}))
    }

    async fn chunk_exists(
        &self,
        request: Request<ChunkExistsRequest>,
    ) -> Result<Response<ChunkExistsResponse>, Status> {
        let req = request.into_inner();
        let store = get_chunk_store(&req.bdev_name)?;
        let exists = store.has_chunk(&req.chunk_id);
        Ok(Response::new(ChunkExistsResponse { exists }))
    }

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let req = request.into_inner();
        let store = get_chunk_store(&req.bdev_name)?;
        let chunk_ids = store.list_chunks();
        Ok(Response::new(ListChunksResponse { chunk_ids }))
    }

    async fn garbage_collect(
        &self,
        request: Request<GarbageCollectRequest>,
    ) -> Result<Response<GarbageCollectResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        let store = get_chunk_store(&req.bdev_name)?;
        let live_set: HashSet<String> = req.live_chunk_ids.into_iter().collect();
        let all_chunks = store.list_chunks();

        let mut deleted: u32 = 0;
        let mut errors: u32 = 0;
        for chunk_id in &all_chunks {
            if !live_set.contains(chunk_id) {
                match store.delete_chunk(chunk_id) {
                    Ok(()) => deleted += 1,
                    Err(e) => {
                        log::warn!("gc: failed to delete orphan chunk {}: {}", chunk_id, e);
                        errors += 1;
                    }
                }
            }
        }

        Ok(Response::new(GarbageCollectResponse { deleted, errors }))
    }

    // ========================================================================
    // Inter-Dataplane Chunk Transfer (used for replication)
    // ========================================================================

    async fn put_chunk(
        &self,
        request: Request<PutChunkRequest>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no writes accepted",
            ));
        }

        let req = request.into_inner();
        if req.chunk_id.is_empty() {
            return Err(Status::invalid_argument("chunk_id is required"));
        }
        if req.data.is_empty() {
            return Err(Status::invalid_argument("data is required"));
        }

        let store = get_any_async_chunk_store()?;
        store
            .put(&req.chunk_id, &req.data)
            .await
            .map_err(|e| Status::internal(format!("put_chunk: {e}")))?;

        log::debug!(
            "put_chunk: stored chunk {} ({} bytes)",
            &req.chunk_id[..std::cmp::min(16, req.chunk_id.len())],
            req.data.len(),
        );

        Ok(Response::new(PutChunkResponse {}))
    }

    async fn get_chunk(
        &self,
        request: Request<GetChunkRequest>,
    ) -> Result<Response<GetChunkResponse>, Status> {
        let req = request.into_inner();
        if req.chunk_id.is_empty() {
            return Err(Status::invalid_argument("chunk_id is required"));
        }

        let store = get_any_async_chunk_store()?;
        let data = store
            .get(&req.chunk_id)
            .await
            .map_err(|e| Status::not_found(format!("get_chunk: {e}")))?;

        log::debug!(
            "get_chunk: retrieved chunk {} ({} bytes)",
            &req.chunk_id[..std::cmp::min(16, req.chunk_id.len())],
            data.len(),
        );

        Ok(Response::new(GetChunkResponse { data }))
    }

    async fn put_sub_block(
        &self,
        request: Request<PutSubBlockRequest>,
    ) -> Result<Response<PutSubBlockResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no writes accepted",
            ));
        }

        let req = request.into_inner();
        let bdev_name = crate::bdev::novastor_bdev::get_backend_bdev_name()
            .map_err(|e| Status::failed_precondition(format!("backend not ready: {e}")))?;

        let chunk_base = req.chunk_index * crate::bdev::sub_block::CHUNK_SIZE as u64;
        let sb_offset = crate::bdev::sub_block::backend_sub_block_offset(
            chunk_base,
            req.sub_block_index as usize,
        );

        crate::spdk::reactor_dispatch::bdev_write_async(bdev_name, sb_offset, &req.data)
            .await
            .map_err(|e| Status::internal(format!("sub-block write failed: {e}")))?;

        log::debug!(
            "put_sub_block: vol={} chunk={} sb={} ({}B)",
            req.volume_id,
            req.chunk_index,
            req.sub_block_index,
            req.data.len()
        );

        Ok(Response::new(PutSubBlockResponse {}))
    }

    // ========================================================================
    // Backend Management (delegates to StorageBackend trait implementations)
    // ========================================================================

    async fn init_backend(
        &self,
        request: Request<InitBackendRequest>,
    ) -> Result<Response<InitBackendResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        log::info!(
            "gRPC init_backend: type={}, config={}",
            req.backend_type,
            req.config_json
        );
        let config: serde_json::Value = serde_json::from_str(&req.config_json)
            .map_err(|e| Status::invalid_argument(format!("invalid config JSON: {e}")))?;

        match req.backend_type.as_str() {
            "raw" => {
                // Raw device backend: attach a block device via io_uring bdev.
                // This works on all platforms (no IOMMU/vfio-pci required).
                // Config: {"device_path": "/dev/nvme0n1", "name": "raw_nvme0"}
                let device_path = config["device_path"]
                    .as_str()
                    .ok_or_else(|| Status::invalid_argument("device_path required in config"))?;
                let name = config["name"]
                    .as_str()
                    .ok_or_else(|| Status::invalid_argument("name required in config"))?;

                // If bdev already exists, skip creation.
                if self.bdev_manager.get_bdev(name).is_some() {
                    log::info!("init_backend: raw bdev '{}' already exists, reusing", name);
                } else {
                    let local_config = LocalBdevConfig {
                        name: name.to_string(),
                        device_path: device_path.to_string(),
                        block_size: 512,
                    };
                    self.bdev_manager
                        .create_aio_bdev(&local_config)
                        .map_err(|e| Status::internal(format!("create raw uring bdev: {e}")))?;
                }

                // Register backend if not already present.
                let mut backends = grpc_backends().lock().unwrap();
                if !backends.contains_key(name) {
                    backends.insert(
                        name.to_string(),
                        Arc::new(crate::backend::raw_disk::RawDiskBackend::new()),
                    );
                }

                log::info!(
                    "init_backend: raw uring backend '{}' on {}",
                    name,
                    device_path
                );
                Ok(Response::new(InitBackendResponse {}))
            }
            "lvm" => {
                // LVM backend: create an SPDK lvol store on an existing bdev.
                // Config: {"bdev_name": "NVMe0n1", "lvs_name": "novastor_lvs", "cluster_size": 1048576}
                let bdev_name = config["bdev_name"]
                    .as_str()
                    .ok_or_else(|| Status::invalid_argument("bdev_name required in config"))?;
                let lvs_name = config["lvs_name"]
                    .as_str()
                    .ok_or_else(|| Status::invalid_argument("lvs_name required in config"))?;
                let cluster_size = config["cluster_size"].as_u64().unwrap_or(1024 * 1024) as u32;

                let bs_config = BlobstoreConfig {
                    base_bdev: bdev_name.to_string(),
                    cluster_size,
                };
                self.bdev_manager
                    .create_lvol_store(&bs_config)
                    .map_err(|e| Status::internal(format!("create lvol store: {e}")))?;

                let backend = Arc::new(crate::backend::lvm::LvmBackend::new(lvs_name));
                grpc_backends()
                    .lock()
                    .unwrap()
                    .insert(lvs_name.to_string(), backend);

                log::info!(
                    "init_backend: lvm backend '{}' on bdev '{}'",
                    lvs_name,
                    bdev_name
                );
                Ok(Response::new(InitBackendResponse {}))
            }
            "file" => {
                // File backend: create an SPDK AIO bdev backed by a file on a mounted filesystem.
                // Config: {"path": "/var/lib/novastor/pool1", "capacity_bytes": 1099511627776, "name": "file_pool1"}
                let path = config["path"]
                    .as_str()
                    .ok_or_else(|| Status::invalid_argument("path required in config"))?;
                let capacity_bytes = config["capacity_bytes"]
                    .as_u64()
                    .ok_or_else(|| Status::invalid_argument("capacity_bytes required in config"))?;
                let name = config["name"].as_str().unwrap_or("file_backend");

                // Create the backing file and AIO bdev.
                std::fs::create_dir_all(path)
                    .map_err(|e| Status::internal(format!("create directory {}: {}", path, e)))?;
                let backing_file = format!("{}/novastor-chunks.dat", path);
                {
                    let f = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&backing_file)
                        .map_err(|e| Status::internal(format!("create backing file: {e}")))?;
                    let current_size = f.metadata().map(|m| m.len()).unwrap_or(0);
                    if current_size < capacity_bytes {
                        f.set_len(capacity_bytes)
                            .map_err(|e| Status::internal(format!("set backing file size: {e}")))?;
                    }
                }

                let aio_config = LocalBdevConfig {
                    name: name.to_string(),
                    device_path: backing_file.clone(),
                    block_size: 4096,
                };
                self.bdev_manager
                    .create_aio_bdev(&aio_config)
                    .map_err(|e| Status::internal(format!("create AIO bdev: {e}")))?;

                log::info!(
                    "init_backend: file backend '{}' at '{}' ({}B)",
                    name,
                    backing_file,
                    capacity_bytes
                );
                Ok(Response::new(InitBackendResponse {}))
            }
            other => Err(Status::invalid_argument(format!(
                "unknown backend type '{}' — expected raw, lvm, or file",
                other
            ))),
        }
    }

    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        log::info!(
            "gRPC create_volume: backend={}, name={}, size={}, frontend_only={}",
            req.backend_type,
            req.name,
            req.size_bytes,
            req.frontend_only,
        );

        // If frontend_only and no chunk engine is initialised yet, create a
        // remote-only ChunkEngine backed by a NullChunkStore. This allows the
        // node to present NVMe-oF targets without local storage — all I/O
        // routes to remote backends via NDP/CRUSH placement.
        if req.frontend_only {
            if crate::bdev::novastor_bdev::get_chunk_engine().is_err() {
                log::info!(
                    "create_volume: frontend-only mode — initialising remote-only ChunkEngine"
                );
                use crate::backend::chunk_store::NullChunkStore;
                use crate::chunk::engine::ChunkEngine;
                use crate::metadata::topology::ClusterMap;

                let null_store = std::sync::Arc::new(NullChunkStore);
                // Empty topology — CRUSH placement will use remote nodes once
                // topology is updated via UpdateTopology RPCs from the controller.
                let topology = ClusterMap::new(0);
                let node_id = format!("frontend-{}", uuid::Uuid::new_v4());
                let engine = std::sync::Arc::new(ChunkEngine::new(node_id, null_store, topology));
                let handle = tokio::runtime::Handle::current();
                crate::bdev::novastor_bdev::set_chunk_engine(engine, handle);
            }
        }

        // ALL volumes go through novastor_bdev → ChunkEngine → Backend.
        // The backend type (raw/lvm/file) only matters at init_backend time.
        // By create_volume, the ChunkEngine is already wired to the underlying
        // BdevChunkStore (or NullChunkStore on frontend-only nodes).
        // The novastor_bdev creates a virtual SPDK bdev.
        let bdev = crate::bdev::novastor_bdev::create(&req.name, req.size_bytes)
            .map_err(|e| Status::internal(format!("create novastor bdev: {e}")))?;

        // Register volume on ALL topology nodes via NDP.
        // NDP connections are pre-warmed on topology update, so broadcast only
        // sends messages (no TCP connect latency). Wait with 5s timeout to
        // ensure all nodes know the volume before I/O starts.
        if let Ok(engine) = crate::bdev::novastor_bdev::get_chunk_engine() {
            let ndp_pool = engine.ndp_pool().clone();
            let vol_name = req.name.clone();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                ndp_pool.broadcast_register_volume(&vol_name),
            )
            .await;
            match result {
                Ok(registered) => {
                    log::info!(
                        "create_volume: registered '{}' on {} NDP peers",
                        vol_name,
                        registered
                    );
                }
                Err(_) => {
                    log::warn!(
                        "create_volume: NDP registration timed out for '{}' — topology push will catch up",
                        vol_name
                    );
                }
            }
        }

        Ok(Response::new(CreateVolumeResponse {
            name: bdev,
            size_bytes: req.size_bytes,
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        if is_fenced() {
            return Err(Status::unavailable(
                "dataplane is fenced — no mutations accepted",
            ));
        }
        let req = request.into_inner();
        // ALL volumes are novastor_bdevs on top of ChunkEngine.
        crate::bdev::novastor_bdev::destroy(&req.name)
            .map_err(|e| Status::internal(format!("destroy novastor bdev: {e}")))?;
        // Clean up the chunk map for this volume (in-memory and persistent).
        if let Some(maps) = crate::bdev::novastor_bdev::volume_chunk_maps_ref() {
            maps.write().unwrap().remove(&req.name);
        }
        if let Some(store) = crate::bdev::novastor_bdev::metadata_store_ref() {
            if let Err(e) = store.delete_volume(&req.name) {
                log::warn!(
                    "delete_volume: failed to remove volume '{}' from metadata store: {}",
                    req.name,
                    e
                );
            }
            // Delete all chunk map entries for this volume.
            match store.list_chunk_map(&req.name) {
                Ok(entries) => {
                    for entry in &entries {
                        if let Err(e) = store.delete_chunk_map(&req.name, entry.chunk_index) {
                            log::warn!(
                                "delete_volume: failed to remove chunk map entry (vol={}, idx={}): {}",
                                req.name, entry.chunk_index, e,
                            );
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "delete_volume: failed to list chunk maps for '{}': {}",
                        req.name,
                        e
                    );
                }
            }
        }
        Ok(Response::new(DeleteVolumeResponse {}))
    }

    // ========================================================================
    // Policy Engine
    // ========================================================================

    async fn set_volume_policy(
        &self,
        request: Request<SetVolumePolicyRequest>,
    ) -> Result<Response<SetVolumePolicyResponse>, Status> {
        let req = request.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id is required"));
        }
        if req.desired_replicas == 0 && req.data_shards == 0 {
            return Err(Status::invalid_argument(
                "either desired_replicas or data_shards must be > 0",
            ));
        }

        let policy_engine = get_any_policy_engine()?;
        let policy = if req.data_shards > 0 && req.parity_shards > 0 {
            crate::policy::types::VolumePolicy::new_ec(
                req.volume_id.clone(),
                req.data_shards,
                req.parity_shards,
            )
        } else {
            crate::policy::types::VolumePolicy::new(req.volume_id.clone(), req.desired_replicas)
        };
        policy_engine.set_policy(policy).await;

        // Also update the ChunkEngine's protection so writes use the new
        // scheme immediately (not just the PolicyEngine's background reconcile).
        if let Ok(chunk_engine) = crate::bdev::novastor_bdev::get_chunk_engine() {
            if req.data_shards > 0 && req.parity_shards > 0 {
                chunk_engine.set_protection(crate::metadata::types::Protection::ErasureCoding {
                    data_shards: req.data_shards,
                    parity_shards: req.parity_shards,
                });
                log::info!(
                    "set_volume_policy: volume={} EC {}/{} (ChunkEngine protection updated)",
                    req.volume_id,
                    req.data_shards,
                    req.parity_shards,
                );
            } else {
                chunk_engine.set_protection(crate::metadata::types::Protection::Replication {
                    factor: req.desired_replicas,
                });
                log::info!(
                    "set_volume_policy: volume={} replicas={} (ChunkEngine protection updated)",
                    req.volume_id,
                    req.desired_replicas,
                );
            }
        }

        Ok(Response::new(SetVolumePolicyResponse { accepted: true }))
    }

    // ========================================================================
    // Topology
    // ========================================================================

    async fn update_topology(
        &self,
        request: Request<UpdateTopologyRequest>,
    ) -> Result<Response<UpdateTopologyResponse>, Status> {
        let req = request.into_inner();
        use crate::metadata::topology::{Backend, BackendType, ClusterMap, Node, NodeStatus};

        let engine = crate::bdev::novastor_bdev::get_chunk_engine()
            .map_err(|e| Status::internal(format!("chunk engine not ready: {e}")))?;

        // Build a shared helper that parses proto nodes into Node structs.
        let parse_nodes = |nodes: &[TopologyNode]| -> Vec<Node> {
            nodes
                .iter()
                .map(|proto_node| {
                    let status = match proto_node.status.as_str() {
                        "online" => NodeStatus::Online,
                        "draining" => NodeStatus::Draining,
                        _ => NodeStatus::Offline,
                    };
                    let backends = proto_node
                        .backends
                        .iter()
                        .map(|b| Backend {
                            id: b.id.clone(),
                            node_id: b.node_id.clone(),
                            capacity_bytes: b.capacity_bytes,
                            used_bytes: b.used_bytes,
                            weight: b.weight,
                            backend_type: match b.backend_type.as_str() {
                                "file" => BackendType::File,
                                "lvm" => BackendType::Lvm,
                                _ => BackendType::Bdev,
                            },
                        })
                        .collect();
                    // Strip port from address if the Go agent included it
                    // (e.g. "192.168.100.13:9100").  The dataplane needs just
                    // the IP for inter-node gRPC connections.
                    let address = if proto_node.address.contains(':') {
                        proto_node
                            .address
                            .split(':')
                            .next()
                            .unwrap_or(&proto_node.address)
                            .to_string()
                    } else {
                        proto_node.address.clone()
                    };
                    Node {
                        id: proto_node.node_id.clone(),
                        address,
                        port: proto_node.port as u16,
                        backends,
                        status,
                    }
                })
                .collect()
        };

        // Build ClusterMap using set_nodes so the generation is set once from
        // the proto field rather than being incremented per add_node call.
        let mut topology = ClusterMap::new(req.generation);
        topology.set_nodes(parse_nodes(&req.nodes));

        // Parse volumes from proto and attach them to the topology.
        {
            use crate::metadata::types::{Protection, VolumeDefinition, VolumeStatus};
            let volumes: Vec<VolumeDefinition> = req
                .volumes
                .iter()
                .map(|vi| {
                    let protection = if vi.protection_type == "erasure_coding" {
                        Protection::ErasureCoding {
                            data_shards: vi.data_shards,
                            parity_shards: vi.parity_shards,
                        }
                    } else {
                        Protection::Replication {
                            factor: vi.rep_factor.max(1),
                        }
                    };
                    let status = match vi.status.as_str() {
                        "deleting" => VolumeStatus::Deleting,
                        "creating" => VolumeStatus::Creating,
                        "error" => VolumeStatus::Error,
                        _ => VolumeStatus::Available,
                    };
                    VolumeDefinition {
                        id: vi.name.clone(),
                        name: vi.name.clone(),
                        size_bytes: vi.size_bytes,
                        protection,
                        status,
                        created_at: 0,
                        chunk_count: vi.size_bytes.saturating_add(4 * 1024 * 1024 - 1)
                            / (4 * 1024 * 1024),
                    }
                })
                .collect();
            topology.set_volumes(volumes);
        }

        let (accepted, _old_topo) = engine.update_topology(topology);
        if accepted {
            // Register volume hashes for NDP lookups so peer nodes can route
            // NDP requests to this node even before a bdev is created.
            for vi in &req.volumes {
                crate::transport::ndp_server::register_volume_hash(&vi.name);
            }

            // Also update the policy engine's topology if it exists.
            if let Ok(pe) = get_any_policy_engine() {
                let mut topo2 = ClusterMap::new(req.generation);
                topo2.set_nodes(parse_nodes(&req.nodes));
                pe.update_topology(topo2).await;
            }

            // Pre-warm NDP connections to all peer nodes so that
            // CreateVolume registration and data I/O don't pay TCP connect
            // latency. Done in background (non-blocking).
            let node_id = engine.node_id().to_string();
            let ndp_pool = engine.ndp_pool().clone();
            let peer_addrs: Vec<String> = req
                .nodes
                .iter()
                .filter(|n| n.node_id != node_id)
                .map(|n| format!("{}:4500", n.address))
                .collect();
            // Pre-warm tokio NDP connections (for flush/write fan-out).
            let peer_addrs_clone = peer_addrs.clone();
            tokio::spawn(async move {
                ndp_pool.warm_connections(&peer_addrs_clone).await;
            });

            // Connect reactor NDP peers. Poller at 50μs avoids reactor stall.
            // Dead sockets cleaned up in ndp_sock_cb. Lazy-connect in send_read
            // handles peers that disconnect between topology updates.
            #[cfg(feature = "spdk-sys")]
            {
                let addrs = peer_addrs;
                crate::spdk::reactor_dispatch::send_to_reactor(move || {
                    for addr in &addrs {
                        if let Some((ip, port_str)) = addr.rsplit_once(':') {
                            if let Ok(port) = port_str.parse::<u16>() {
                                unsafe {
                                    crate::chunk::reactor_ndp::connect_peer(ip, port);
                                }
                            }
                        }
                    }
                });
            }
        }

        Ok(Response::new(UpdateTopologyResponse { accepted }))
    }

    // ========================================================================
    // Metadata Sync
    // ========================================================================

    async fn get_chunk_maps(
        &self,
        request: Request<GetChunkMapsRequest>,
    ) -> Result<Response<GetChunkMapsResponse>, Status> {
        let _req = request.into_inner();
        let maps = crate::bdev::novastor_bdev::volume_chunk_maps()
            .read()
            .unwrap();
        let mut entries = Vec::new();
        let mut volume_count = 0u32;
        for (vol_id, chunk_map) in maps.iter() {
            volume_count += 1;
            for (idx, entry_opt) in chunk_map.iter().enumerate() {
                if let Some(entry) = entry_opt {
                    entries.push(ChunkMapEntryProto {
                        volume_id: vol_id.clone(),
                        chunk_index: idx as u64,
                        chunk_id: entry.chunk_id.clone(),
                        offset: entry.chunk_index * 4194304,
                        length: 4194304,
                    });
                }
            }
        }
        Ok(Response::new(GetChunkMapsResponse {
            entries,
            volume_count,
        }))
    }

    async fn sync_chunk_maps(
        &self,
        request: Request<SyncChunkMapsRequest>,
    ) -> Result<Response<SyncChunkMapsResponse>, Status> {
        let req = request.into_inner();
        let since = req.since_generation;

        let mut updates = Vec::new();

        if let Some(store) = crate::bdev::novastor_bdev::get_metadata_store() {
            if let Ok(volumes) = store.list_volumes() {
                for vol in &volumes {
                    if let Ok(chunks) = store.list_chunk_map(&vol.id) {
                        for cm in chunks {
                            if cm.generation > since {
                                updates.push(ChunkMapUpdate {
                                    volume_id: vol.id.clone(),
                                    chunk_index: cm.chunk_index,
                                    dirty_bitmap: cm.dirty_bitmap,
                                    placements: cm.placements.clone(),
                                    generation: cm.generation,
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(Response::new(SyncChunkMapsResponse { updates }))
    }

    // ========================================================================
    // Health & Fencing
    // ========================================================================

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit: String::new(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        LAST_HEARTBEAT.store(req.timestamp, Ordering::Relaxed);
        log::debug!("heartbeat from node {}", req.node_id);
        Ok(Response::new(HeartbeatResponse {
            fenced: is_fenced(),
            status: "ok".to_string(),
        }))
    }

    // ========================================================================
    // EC Shard Reconstruction
    // ========================================================================

    async fn reconstruct_shard(
        &self,
        request: Request<ReconstructShardRequest>,
    ) -> Result<Response<ReconstructShardResponse>, Status> {
        let req = request.into_inner();
        if req.chunk_id.is_empty() {
            return Err(Status::invalid_argument("chunk_id is required"));
        }
        if req.data_shards == 0 {
            return Err(Status::invalid_argument("data_shards must be > 0"));
        }
        if req.parity_shards == 0 {
            return Err(Status::invalid_argument("parity_shards must be > 0"));
        }
        let total = req.data_shards + req.parity_shards;
        if req.shard_index >= total {
            return Err(Status::invalid_argument(format!(
                "shard_index {} out of range (total shards {})",
                req.shard_index, total
            )));
        }

        let policy_engine = get_any_policy_engine()?;

        let shard_index = req.shard_index as usize;
        let total_shards = total as usize;
        let mut shard_addrs: Vec<(usize, String, String)> = Vec::new();

        for idx in 0..total_shards {
            if idx == shard_index {
                continue;
            }
            let shard_id = format!("{}:shard:{}", req.chunk_id, idx);
            // Use the policy engine's location store (accessed via public methods).
            // We check if the async chunk store has this shard by querying
            // the store. For remote shards we'd need the location store.
            // For now, rely on the location store exposed through the engine.
            //
            // Since PolicyEngine doesn't expose location_store directly, we
            // check via the chunk store for local availability, and rely on
            // the caller (Go agent) providing target_node info.
            let store = get_any_async_chunk_store()?;
            match store.exists(&shard_id).await {
                Ok(true) => {
                    let node_id = policy_engine.node_id().to_string();
                    shard_addrs.push((idx, node_id, String::new()));
                }
                _ => {
                    // Shard not local — skip. The background reconcile loop
                    // handles multi-node reconstruction via the location store.
                }
            }
        }

        if shard_addrs.len() < req.data_shards as usize {
            return Err(Status::failed_precondition(format!(
                "insufficient local surviving shards: have {}, need {} data shards",
                shard_addrs.len(),
                req.data_shards
            )));
        }

        // Perform reconstruction using ChunkOperations-style logic inline.
        use crate::backend::chunk_store::ChunkHeader;

        let mut available: Vec<(usize, Vec<u8>)> = Vec::new();
        let store = get_any_async_chunk_store()?;
        for (idx, _node_id, _addr) in &shard_addrs {
            let shard_id = format!("{}:shard:{}", req.chunk_id, idx);
            match store.get(&shard_id).await {
                Ok(shard_with_header) => {
                    if shard_with_header.len() < ChunkHeader::SIZE {
                        continue;
                    }
                    let header_bytes: [u8; ChunkHeader::SIZE] = shard_with_header
                        [..ChunkHeader::SIZE]
                        .try_into()
                        .map_err(|_| Status::internal("shard header read failed"))?;
                    let header = ChunkHeader::from_bytes(&header_bytes)
                        .map_err(|e| Status::internal(format!("shard header parse: {e}")))?;
                    let data_len = header.data_len as usize;
                    if shard_with_header.len() < ChunkHeader::SIZE + data_len {
                        continue;
                    }
                    let raw =
                        shard_with_header[ChunkHeader::SIZE..ChunkHeader::SIZE + data_len].to_vec();
                    available.push((*idx, raw));
                }
                Err(e) => {
                    log::warn!("reconstruct_shard: failed to read shard {}: {}", idx, e);
                }
            }
        }

        if available.len() < req.data_shards as usize {
            return Err(Status::internal(format!(
                "could not read enough shards: got {}, need {}",
                available.len(),
                req.data_shards
            )));
        }

        let reconstructed = if shard_index < req.data_shards as usize {
            let mut originals: Vec<(usize, &[u8])> = Vec::new();
            let mut recovery: Vec<(usize, &[u8])> = Vec::new();
            for (idx, data) in &available {
                if *idx < req.data_shards as usize {
                    originals.push((*idx, data.as_slice()));
                } else {
                    recovery.push((*idx - req.data_shards as usize, data.as_slice()));
                }
            }
            let recovered = reed_solomon_simd::decode(
                req.data_shards as usize,
                req.parity_shards as usize,
                originals,
                recovery,
            )
            .map_err(|e| Status::internal(format!("RS decode failed: {e}")))?;
            recovered
                .get(&shard_index)
                .cloned()
                .ok_or_else(|| Status::internal("RS decode did not produce target shard"))?
        } else {
            let mut data_pieces: Vec<Vec<u8>> = vec![Vec::new(); req.data_shards as usize];
            for (idx, data) in &available {
                if *idx < req.data_shards as usize {
                    data_pieces[*idx] = data.clone();
                }
            }
            if data_pieces.iter().any(|d| d.is_empty()) {
                return Err(Status::internal(
                    "cannot reconstruct parity: missing data shards",
                ));
            }
            let data_refs: Vec<&[u8]> = data_pieces.iter().map(|d| d.as_slice()).collect();
            let parity = reed_solomon_simd::encode(
                req.data_shards as usize,
                req.parity_shards as usize,
                data_refs.into_iter(),
            )
            .map_err(|e| Status::internal(format!("RS encode failed: {e}")))?;
            let parity_idx = shard_index - req.data_shards as usize;
            parity
                .into_iter()
                .nth(parity_idx)
                .ok_or_else(|| Status::internal("RS encode did not produce target parity shard"))?
        };

        // Write the reconstructed shard.
        let header = ChunkHeader {
            magic: *b"NVAC",
            version: 1,
            flags: 0,
            checksum: crc32c::crc32c(&reconstructed),
            data_len: reconstructed.len() as u32,
            _reserved: [0; 2],
        };
        let mut prepared = Vec::with_capacity(ChunkHeader::SIZE + reconstructed.len());
        prepared.extend_from_slice(&header.to_bytes());
        prepared.extend_from_slice(&reconstructed);

        let shard_id = format!("{}:shard:{}", req.chunk_id, shard_index);
        store
            .put(&shard_id, &prepared)
            .await
            .map_err(|e| Status::internal(format!("failed to write reconstructed shard: {e}")))?;

        log::info!(
            "reconstruct_shard: rebuilt shard {} index {} ({} bytes)",
            req.chunk_id,
            shard_index,
            reconstructed.len()
        );

        Ok(Response::new(ReconstructShardResponse { success: true }))
    }
}
