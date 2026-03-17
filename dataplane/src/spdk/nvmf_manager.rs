//! NVMe-oF target and initiator management.
//!
//! All SPDK NVMe-oF calls are dispatched to the reactor thread via
//! `reactor_dispatch::send_to_reactor` / `dispatch_sync`. Async SPDK
//! operations (add_transport, add_listener, start/stop/destroy subsystem)
//! use chained callbacks that signal a `Completion` when the full sequence
//! is done.

use crate::config::{NvmfInitiatorConfig, NvmfTargetConfig};
use crate::error::{DataPlaneError, Result};
use log::{error, info, warn};
use std::collections::HashMap;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex, Once};

use super::context::Completion;
use super::reactor_dispatch::{self, SendPtr};

#[allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    improper_ctypes
)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/spdk_bindings.rs"));
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SubsystemInfo {
    pub nqn: String,
    pub bdev_name: String,
    pub listen_address: String,
    pub listen_port: u16,
    pub ana_group_id: u32,
    pub ana_state: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct InitiatorInfo {
    pub nqn: String,
    pub remote_address: String,
    pub remote_port: u16,
    pub local_bdev_name: String,
}

pub struct NvmfManager {
    subsystems: Mutex<HashMap<String, SubsystemInfo>>,
    initiators: Mutex<HashMap<String, InitiatorInfo>>,
    next_local_port: Mutex<u16>,
    /// Cached NVMe-oF target pointer (set after init_transport).
    tgt_ptr: Mutex<Option<SendPtr>>,
    /// Ensures TCP transport is initialised exactly once.
    transport_once: Once,
    transport_err: Mutex<Option<String>>,
}

// ---------------------------------------------------------------------------
// Context structs for chained SPDK callbacks
// ---------------------------------------------------------------------------

/// Context passed through the create-target callback chain:
///   add_listener_done -> subsystem_start_done -> final completion
struct TargetCreateCtx {
    subsystem: SendPtr,
    trid: Box<ffi::spdk_nvme_transport_id>,
    completion: Arc<Completion<i32>>,
}

// SAFETY: Only dereferenced on the SPDK reactor thread.
unsafe impl Send for TargetCreateCtx {}

/// Context passed through the delete-target callback chain:
///   subsystem_stop_done -> subsystem_destroy_done -> final completion
struct TargetDeleteCtx {
    subsystem: SendPtr,
    completion: Arc<Completion<i32>>,
}

unsafe impl Send for TargetDeleteCtx {}

impl NvmfManager {
    pub fn new(base_port: u16) -> Self {
        Self {
            subsystems: Mutex::new(HashMap::new()),
            initiators: Mutex::new(HashMap::new()),
            next_local_port: Mutex::new(base_port),
            tgt_ptr: Mutex::new(None),
            transport_once: Once::new(),
            transport_err: Mutex::new(None),
        }
    }

    /// Ensure the NVMe-oF TCP transport is initialised. Safe to call multiple
    /// times — the actual initialisation runs exactly once.
    pub fn ensure_transport(&self) -> Result<()> {
        self.transport_once.call_once(|| {
            if let Err(e) = self.init_transport() {
                *self.transport_err.lock().unwrap() = Some(e.to_string());
            }
        });
        if let Some(ref err_msg) = *self.transport_err.lock().unwrap() {
            return Err(DataPlaneError::NvmfTargetError(format!(
                "transport init failed: {err_msg}"
            )));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Transport initialisation
    // -----------------------------------------------------------------------

    /// Initialise the NVMe-oF TCP transport. Must be called once before
    /// creating any targets.
    ///
    /// 1. Gets or creates the NVMe-oF target.
    /// 2. Initialises TCP transport options.
    /// 3. Creates the TCP transport.
    /// 4. Adds the transport to the target (async, waits for callback).
    pub fn init_transport(&self) -> Result<()> {
        info!("initialising NVMe-oF TCP transport");

        // Step 1-3 run synchronously on the reactor to obtain pointers.
        let (tgt_send, transport_send) =
            reactor_dispatch::dispatch_sync(|| -> Result<(SendPtr, SendPtr)> {
                unsafe {
                    // Get or create the NVMe-oF target.
                    let mut tgt = ffi::spdk_nvmf_get_first_tgt();
                    if tgt.is_null() {
                        let mut opts: ffi::spdk_nvmf_target_opts = std::mem::zeroed();
                        tgt = ffi::spdk_nvmf_tgt_create(&mut opts);
                        if tgt.is_null() {
                            return Err(DataPlaneError::NvmfTargetError(
                                "spdk_nvmf_tgt_create failed".to_string(),
                            ));
                        }
                        info!("created new NVMe-oF target");
                    }

                    // Initialise TCP transport options.
                    let transport_name = std::ffi::CString::new("TCP").unwrap();
                    let mut transport_opts: ffi::spdk_nvmf_transport_opts = std::mem::zeroed();
                    let ok = ffi::spdk_nvmf_transport_opts_init(
                        transport_name.as_ptr() as *const c_char,
                        &mut transport_opts,
                        std::mem::size_of::<ffi::spdk_nvmf_transport_opts>(),
                    );
                    if !ok {
                        return Err(DataPlaneError::NvmfTargetError(
                            "spdk_nvmf_transport_opts_init failed for TCP".to_string(),
                        ));
                    }

                    // Tune transport: set io_unit_size=131072 (128KB) and
                    // max_qpairs_per_ctrlr=8 for parallel I/O queues.
                    // Field offsets from spdk/nvmf.h spdk_nvmf_transport_opts:
                    //   offset 2: max_qpairs_per_ctrlr (u16)
                    //   offset 12: io_unit_size (u32)
                    let opts_ptr = &mut transport_opts as *mut _ as *mut u8;
                    // max_qpairs_per_ctrlr at offset 2 (u16)
                    *(opts_ptr.add(2) as *mut u16) = 8;
                    // io_unit_size at offset 12 (u32)
                    *(opts_ptr.add(12) as *mut u32) = 131072;
                    info!("NVMe-oF TCP transport opts: max_qpairs_per_ctrlr=8, io_unit_size=128KB");

                    // Create the TCP transport.
                    let transport = ffi::spdk_nvmf_transport_create(
                        transport_name.as_ptr() as *const c_char,
                        &mut transport_opts,
                    );
                    if transport.is_null() {
                        return Err(DataPlaneError::NvmfTargetError(
                            "spdk_nvmf_transport_create failed for TCP".to_string(),
                        ));
                    }

                    Ok((
                        SendPtr::new(tgt as *mut c_void),
                        SendPtr::new(transport as *mut c_void),
                    ))
                }
            })?;

        // Step 4: add_transport is async — use send_to_reactor + Completion.
        let completion = Arc::new(Completion::<i32>::new());
        let comp = completion.clone();

        reactor_dispatch::send_to_reactor(move || {
            unsafe extern "C" fn add_transport_done(cb_arg: *mut c_void, status: i32) {
                let comp = Arc::from_raw(cb_arg as *const Completion<i32>);
                comp.complete(status);
            }

            unsafe {
                let comp_ptr = Arc::into_raw(comp) as *mut c_void;
                ffi::spdk_nvmf_tgt_add_transport(
                    tgt_send.as_ptr() as *mut ffi::spdk_nvmf_tgt,
                    transport_send.as_ptr() as *mut ffi::spdk_nvmf_transport,
                    Some(add_transport_done),
                    comp_ptr,
                );
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::NvmfTargetError(format!(
                "spdk_nvmf_tgt_add_transport failed: rc={rc}"
            )));
        }

        // Create a poll group on the reactor thread. Without a poll group,
        // SPDK never calls accept() on the listen socket and incoming NVMe-oF
        // connections time out. The poll group must be created after the
        // transport is added but before any listeners are registered.
        let tgt_send_pg = tgt_send;
        let pg = reactor_dispatch::dispatch_sync(move || -> Result<SendPtr> {
            unsafe {
                let tgt = tgt_send_pg.as_ptr() as *mut ffi::spdk_nvmf_tgt;
                let poll_group = ffi::spdk_nvmf_poll_group_create(tgt);
                if poll_group.is_null() {
                    return Err(DataPlaneError::NvmfTargetError(
                        "spdk_nvmf_poll_group_create failed".to_string(),
                    ));
                }
                info!("NVMe-oF poll group created on reactor thread");
                Ok(SendPtr::new(poll_group as *mut c_void))
            }
        })?;
        let _ = pg; // Poll group is owned by SPDK target; keep alive.

        // Cache the target pointer for later use.
        *self.tgt_ptr.lock().unwrap() = Some(tgt_send);
        info!("NVMe-oF TCP transport initialised successfully");
        Ok(())
    }

    /// Retrieve the cached NVMe-oF target pointer, or look it up on the
    /// reactor if not yet cached.
    fn get_tgt_ptr(&self) -> Result<SendPtr> {
        if let Some(ptr) = *self.tgt_ptr.lock().unwrap() {
            return Ok(ptr);
        }
        // Fall back to querying SPDK on the reactor.
        let ptr = reactor_dispatch::dispatch_sync(|| -> Result<SendPtr> {
            unsafe {
                let tgt = ffi::spdk_nvmf_get_first_tgt();
                if tgt.is_null() {
                    Err(DataPlaneError::NvmfTargetError(
                        "no NVMe-oF target available (call init_transport first)".to_string(),
                    ))
                } else {
                    Ok(SendPtr::new(tgt as *mut c_void))
                }
            }
        })?;
        *self.tgt_ptr.lock().unwrap() = Some(ptr);
        Ok(ptr)
    }

    // -----------------------------------------------------------------------
    // Target (subsystem) management
    // -----------------------------------------------------------------------

    pub fn create_target(&self, config: &NvmfTargetConfig) -> Result<SubsystemInfo> {
        let nqn = config.nqn();
        info!(
            "creating NVMe-oF target: nqn={}, bdev={}, addr={}:{}",
            nqn, config.bdev_name, config.listen_address, config.listen_port
        );

        // Lazily initialise TCP transport on first target creation.
        self.ensure_transport()?;

        let tgt_ptr = self.get_tgt_ptr()?;

        let nqn_c = std::ffi::CString::new(nqn.as_str())
            .map_err(|e| DataPlaneError::NvmfTargetError(format!("invalid NQN: {e}")))?;
        let bdev_c = std::ffi::CString::new(config.bdev_name.as_str())
            .map_err(|e| DataPlaneError::NvmfTargetError(format!("invalid bdev name: {e}")))?;
        let addr_c = std::ffi::CString::new(config.listen_address.as_str())
            .map_err(|e| DataPlaneError::NvmfTargetError(format!("invalid listen addr: {e}")))?;
        let port_str = config.listen_port.to_string();
        let port_c = std::ffi::CString::new(port_str.as_str()).unwrap();
        let ana_group_id = config.ana_group_id;

        // Phase 1: create subsystem, add namespace, build trid — all synchronous on reactor.
        let (subsystem_send, trid_box) = reactor_dispatch::dispatch_sync(
            move || -> Result<(SendPtr, Box<ffi::spdk_nvme_transport_id>)> {
                unsafe {
                    let tgt = tgt_ptr.as_ptr() as *mut ffi::spdk_nvmf_tgt;

                    // Create the subsystem. num_ns sets max NSID which also constrains
                    // ANA group IDs (ANAGRPID must be ≤ max NSID). Use 1 since each
                    // volume subsystem has exactly one namespace.
                    let subsystem = ffi::spdk_nvmf_subsystem_create(
                        tgt,
                        nqn_c.as_ptr() as *const c_char,
                        ffi::spdk_nvmf_subtype_SPDK_NVMF_SUBTYPE_NVME,
                        1, // num_ns — one namespace per volume
                    );
                    if subsystem.is_null() {
                        return Err(DataPlaneError::NvmfTargetError(format!(
                            "spdk_nvmf_subsystem_create failed for nqn={}",
                            nqn_c.to_str().unwrap_or("?")
                        )));
                    }

                    // Allow any host to connect from any listener address.
                    ffi::spdk_nvmf_subsystem_set_allow_any_host(subsystem, true);
                    ffi::spdk_nvmf_subsystem_allow_any_listener(subsystem, true);

                    // Enable ANA reporting if ana_group_id is set.
                    if ana_group_id > 0 {
                        ffi::spdk_nvmf_subsystem_set_ana_reporting(subsystem, true);
                    }

                    // Add namespace (bdev).
                    let mut ns_opts: ffi::spdk_nvmf_ns_opts = std::mem::zeroed();
                    ffi::spdk_nvmf_ns_opts_get_defaults(
                        &mut ns_opts,
                        std::mem::size_of_val(&ns_opts),
                    );
                    ns_opts.anagrpid = ana_group_id;

                    let ns_id = ffi::spdk_nvmf_subsystem_add_ns_ext(
                        subsystem,
                        bdev_c.as_ptr() as *const c_char,
                        &ns_opts,
                        std::mem::size_of_val(&ns_opts),
                        std::ptr::null(),
                    );
                    if ns_id == 0 {
                        ffi::spdk_nvmf_subsystem_destroy(subsystem, None, std::ptr::null_mut());
                        return Err(DataPlaneError::NvmfTargetError(format!(
                            "spdk_nvmf_subsystem_add_ns_ext failed for bdev={}",
                            bdev_c.to_str().unwrap_or("?")
                        )));
                    }

                    // Build transport ID for the listener.
                    let mut trid: ffi::spdk_nvme_transport_id = std::mem::zeroed();
                    trid.trtype = ffi::spdk_nvme_transport_type_SPDK_NVME_TRANSPORT_TCP;
                    trid.adrfam = ffi::spdk_nvmf_adrfam_SPDK_NVMF_ADRFAM_IPV4;

                    // SPDK v24.09+ uses trstring for transport lookup.
                    let trstring = b"TCP\0";
                    std::ptr::copy_nonoverlapping(
                        trstring.as_ptr() as *const c_char,
                        trid.trstring.as_mut_ptr(),
                        trstring.len().min(trid.trstring.len()),
                    );

                    let addr_bytes = addr_c.as_bytes_with_nul();
                    let port_bytes = port_c.as_bytes_with_nul();
                    std::ptr::copy_nonoverlapping(
                        addr_bytes.as_ptr() as *const c_char,
                        trid.traddr.as_mut_ptr(),
                        addr_bytes.len().min(trid.traddr.len()),
                    );
                    std::ptr::copy_nonoverlapping(
                        port_bytes.as_ptr() as *const c_char,
                        trid.trsvcid.as_mut_ptr(),
                        port_bytes.len().min(trid.trsvcid.len()),
                    );

                    Ok((SendPtr::new(subsystem as *mut c_void), Box::new(trid)))
                }
            },
        )?;

        // Phase 2a: Register transport-level listener via spdk_nvmf_tgt_listen_ext.
        // Both transport and subsystem listeners must use the same address (SPDK
        // requires exact match). The host IP is passed from the Go agent so the
        // kernel NVMe-oF initiator's connection address matches the subsystem ACL.
        {
            // Copy the trid for the listen call; trid_box is used later in phase 2b.
            let trid_copy: ffi::spdk_nvme_transport_id = unsafe { std::ptr::read(&*trid_box) };
            let trid_clone = Box::new(trid_copy);
            let tgt_send_copy = tgt_ptr;

            let rc = reactor_dispatch::dispatch_sync(move || -> Result<i32> {
                unsafe {
                    let mut listen_opts: ffi::spdk_nvmf_listen_opts = std::mem::zeroed();
                    ffi::spdk_nvmf_listen_opts_init(
                        &mut listen_opts,
                        std::mem::size_of::<ffi::spdk_nvmf_listen_opts>(),
                    );

                    let tgt = tgt_send_copy.as_ptr() as *mut ffi::spdk_nvmf_tgt;
                    let rc = ffi::spdk_nvmf_tgt_listen_ext(
                        tgt,
                        &*trid_clone as *const ffi::spdk_nvme_transport_id,
                        &mut listen_opts,
                    );
                    Ok(rc)
                }
            })?;
            // rc=-17 (EEXIST) is expected when multiple targets share the same
            // transport listener address:port. The listener persists across targets.
            if rc != 0 && rc != -17 {
                return Err(DataPlaneError::NvmfTargetError(format!(
                    "spdk_nvmf_tgt_listen_ext failed: rc={rc}"
                )));
            }
        }

        // Phase 2b: add subsystem listener (async) -> start subsystem (async) via chained callbacks.
        let completion = Arc::new(Completion::<i32>::new());
        let ctx = Box::new(TargetCreateCtx {
            subsystem: subsystem_send,
            trid: trid_box,
            completion: completion.clone(),
        });

        reactor_dispatch::send_to_reactor(move || {
            let ctx_ptr = Box::into_raw(ctx) as *mut c_void;
            unsafe {
                let tctx = &*(ctx_ptr as *const TargetCreateCtx);
                let subsystem = tctx.subsystem.as_ptr() as *mut ffi::spdk_nvmf_subsystem;
                let trid_ptr = &*tctx.trid as *const ffi::spdk_nvme_transport_id
                    as *mut ffi::spdk_nvme_transport_id;

                ffi::spdk_nvmf_subsystem_add_listener(
                    subsystem,
                    trid_ptr,
                    Some(listener_done_cb),
                    ctx_ptr,
                );
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::NvmfTargetError(format!(
                "create_target callback chain failed: rc={rc}"
            )));
        }

        let sub_info = SubsystemInfo {
            nqn: nqn.clone(),
            bdev_name: config.bdev_name.clone(),
            listen_address: config.listen_address.clone(),
            listen_port: config.listen_port,
            ana_group_id: config.ana_group_id,
            ana_state: config.ana_state.clone(),
        };
        self.subsystems
            .lock()
            .unwrap()
            .insert(nqn.clone(), sub_info.clone());

        Ok(sub_info)
    }

    pub fn delete_target(&self, volume_id: &str) -> Result<()> {
        let nqn = format!("nqn.2024-01.io.novastor:volume-{}", volume_id);
        info!("deleting NVMe-oF target: nqn={}", nqn);

        let tgt_ptr = self.get_tgt_ptr()?;

        let nqn_owned = nqn.clone();

        // Phase 1: find the subsystem on the reactor.
        let subsystem_ptr = reactor_dispatch::dispatch_sync(move || -> Result<Option<SendPtr>> {
            let nqn_c = std::ffi::CString::new(nqn_owned.as_str()).unwrap();
            unsafe {
                let tgt = tgt_ptr.as_ptr() as *mut ffi::spdk_nvmf_tgt;
                let subsystem =
                    ffi::spdk_nvmf_tgt_find_subsystem(tgt, nqn_c.as_ptr() as *const c_char);
                if subsystem.is_null() {
                    Ok(None)
                } else {
                    Ok(Some(SendPtr::new(subsystem as *mut c_void)))
                }
            }
        })?;

        // Remove from the hashmap immediately so list_subsystems reflects the deletion.
        self.subsystems.lock().unwrap().remove(&nqn);

        let subsystem_send = match subsystem_ptr {
            Some(p) => p,
            None => {
                // Already gone from SPDK.
                return Ok(());
            }
        };

        // Phase 2: stop subsystem (async) -> destroy subsystem (async) via chained callbacks.
        let completion = Arc::new(Completion::<i32>::new());
        let ctx = Box::new(TargetDeleteCtx {
            subsystem: subsystem_send,
            completion: completion.clone(),
        });

        reactor_dispatch::send_to_reactor(move || {
            let ctx_ptr = Box::into_raw(ctx) as *mut c_void;
            // SAFETY: retry_subsystem_stop takes ownership of ctx_ptr on failure paths.
            unsafe { retry_subsystem_stop(ctx_ptr) };
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::NvmfTargetError(format!(
                "delete_target callback chain failed: rc={rc}"
            )));
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Initiator management
    // -----------------------------------------------------------------------

    pub fn connect_initiator(&self, config: &NvmfInitiatorConfig) -> Result<InitiatorInfo> {
        info!(
            "connecting NVMe-oF initiator: nqn={}, remote={}:{}, bdev_name={}",
            config.nqn, config.remote_address, config.remote_port, config.bdev_name
        );

        // Use SPDK's native RPC `bdev_nvme_attach_controller` to create a
        // properly named bdev for the remote NVMe-oF target.  The native
        // RPC server listens on /var/tmp/spdk.sock (started by spdk_app_start).
        let bdev_name = Self::attach_controller_via_native_rpc(
            &config.bdev_name,
            &config.remote_address,
            config.remote_port,
            &config.nqn,
        )?;

        info!(
            "NVMe-oF initiator attached: bdev={}, remote={}:{}",
            bdev_name, config.remote_address, config.remote_port
        );

        let info = InitiatorInfo {
            nqn: config.nqn.clone(),
            remote_address: config.remote_address.clone(),
            remote_port: config.remote_port,
            local_bdev_name: bdev_name.clone(),
        };
        self.initiators
            .lock()
            .unwrap()
            .insert(bdev_name, info.clone());
        Ok(info)
    }

    /// Call SPDK's native `bdev_nvme_attach_controller` via SPDK's internal
    /// RPC socket (/var/tmp/spdk.sock). This is Rust→SPDK communication
    /// (not Go→Rust), so it does not violate invariant #5. Creates a
    /// properly registered bdev for the remote NVMe-oF target.
    fn attach_controller_via_native_rpc(
        name: &str,
        addr: &str,
        port: u16,
        subnqn: &str,
    ) -> Result<String> {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let rpc_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "bdev_nvme_attach_controller",
            "params": {
                "name": name,
                "trtype": "TCP",
                "adrfam": "IPv4",
                "traddr": addr,
                "trsvcid": port.to_string(),
                "subnqn": subnqn,
                "nr_io_queues": 4
            }
        });

        let sock_path = "/var/tmp/spdk.sock";
        let mut stream = UnixStream::connect(sock_path).map_err(|e| {
            DataPlaneError::NvmfInitiatorError(format!(
                "connecting to SPDK native RPC at {sock_path}: {e}"
            ))
        })?;

        // SPDK's native JSON-RPC expects newline-terminated JSON.
        let mut msg = serde_json::to_string(&rpc_request).unwrap();
        msg.push('\n');
        stream.write_all(msg.as_bytes()).map_err(|e| {
            DataPlaneError::NvmfInitiatorError(format!("writing to SPDK native RPC: {e}"))
        })?;

        // Read the response.
        let mut response = String::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = stream.read(&mut buf).map_err(|e| {
                DataPlaneError::NvmfInitiatorError(format!("reading from SPDK native RPC: {e}"))
            })?;
            if n == 0 {
                break;
            }
            response.push_str(&String::from_utf8_lossy(&buf[..n]));
            // Check if we have a complete JSON response.
            if response.contains('\n')
                || serde_json::from_str::<serde_json::Value>(&response).is_ok()
            {
                break;
            }
        }

        let resp: serde_json::Value = serde_json::from_str(&response).map_err(|e| {
            DataPlaneError::NvmfInitiatorError(format!(
                "parsing SPDK native RPC response: {e}, raw: {response}"
            ))
        })?;

        if let Some(err) = resp.get("error") {
            return Err(DataPlaneError::NvmfInitiatorError(format!(
                "SPDK bdev_nvme_attach_controller failed: {}",
                err
            )));
        }

        // The result is an array of bdev names, e.g. ["replica_xyz_n1"]
        if let Some(result) = resp.get("result") {
            if let Some(names) = result.as_array() {
                if let Some(first) = names.first().and_then(|v| v.as_str()) {
                    return Ok(first.to_string());
                }
            }
            // Single string result
            if let Some(s) = result.as_str() {
                return Ok(s.to_string());
            }
        }

        // If result doesn't contain bdev names, use the conventional name
        // pattern: <name>n1
        Ok(format!("{name}n1"))
    }

    pub fn disconnect_initiator(&self, bdev_name: &str) -> Result<()> {
        info!("disconnecting NVMe-oF initiator: bdev={}", bdev_name);

        // Extract the controller name from the bdev name (strip "n1" suffix).
        let ctrl_name = bdev_name.strip_suffix("n1").unwrap_or(bdev_name);

        // Use SPDK's native RPC to detach the controller.
        Self::detach_controller_via_native_rpc(ctrl_name)?;

        self.initiators.lock().unwrap().remove(bdev_name);
        Ok(())
    }

    /// Call SPDK's native `bdev_nvme_detach_controller` via SPDK's internal
    /// RPC socket to cleanly remove the controller and its bdevs.
    fn detach_controller_via_native_rpc(name: &str) -> Result<()> {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let rpc_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "bdev_nvme_detach_controller",
            "params": {
                "name": name
            }
        });

        let sock_path = "/var/tmp/spdk.sock";
        let mut stream = UnixStream::connect(sock_path).map_err(|e| {
            DataPlaneError::NvmfInitiatorError(format!(
                "connecting to SPDK native RPC at {sock_path}: {e}"
            ))
        })?;

        let mut msg = serde_json::to_string(&rpc_request).unwrap();
        msg.push('\n');
        stream.write_all(msg.as_bytes()).map_err(|e| {
            DataPlaneError::NvmfInitiatorError(format!("writing to SPDK native RPC: {e}"))
        })?;

        let mut response = String::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = stream.read(&mut buf).map_err(|e| {
                DataPlaneError::NvmfInitiatorError(format!("reading from SPDK native RPC: {e}"))
            })?;
            if n == 0 {
                break;
            }
            response.push_str(&String::from_utf8_lossy(&buf[..n]));
            if response.contains('\n')
                || serde_json::from_str::<serde_json::Value>(&response).is_ok()
            {
                break;
            }
        }

        // Best-effort: log errors but don't fail hard on disconnect.
        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&response) {
            if let Some(err) = resp.get("error") {
                warn!("SPDK bdev_nvme_detach_controller warning: {}", err);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Convenience / query methods
    // -----------------------------------------------------------------------

    pub fn allocate_local_port(&self) -> u16 {
        let mut port = self.next_local_port.lock().unwrap();
        let p = *port;
        *port += 1;
        p
    }

    pub fn export_local(&self, bdev_name: &str, volume_id: &str) -> Result<SubsystemInfo> {
        let port = self.allocate_local_port();
        let config = NvmfTargetConfig {
            volume_id: volume_id.to_string(),
            bdev_name: bdev_name.to_string(),
            listen_address: "127.0.0.1".to_string(),
            listen_port: port,
            ana_group_id: 0,
            ana_state: "optimized".to_string(),
        };
        self.create_target(&config)
    }

    pub fn list_subsystems(&self) -> Vec<SubsystemInfo> {
        self.subsystems.lock().unwrap().values().cloned().collect()
    }

    pub fn list_initiators(&self) -> Vec<InitiatorInfo> {
        self.initiators.lock().unwrap().values().cloned().collect()
    }

    pub fn get_subsystem(&self, nqn: &str) -> Option<SubsystemInfo> {
        self.subsystems.lock().unwrap().get(nqn).cloned()
    }

    pub fn set_ana_state(&self, nqn: &str, ana_group_id: u32, ana_state: &str) -> Result<()> {
        let mut subsystems = self.subsystems.lock().unwrap();
        let sub_info = subsystems.get_mut(nqn).ok_or_else(|| {
            DataPlaneError::NvmfTargetError(format!("subsystem not found: {}", nqn))
        })?;
        info!(
            "setting ANA state: nqn={}, ana_group_id={}, ana_state={}",
            nqn, ana_group_id, ana_state
        );

        let nqn_c = std::ffi::CString::new(nqn)
            .map_err(|e| DataPlaneError::NvmfTargetError(format!("invalid NQN: {e}")))?;
        let ana_state_owned = ana_state.to_string();

        let tgt_ptr = match *self.tgt_ptr.lock().unwrap() {
            Some(p) => p,
            None => {
                return Err(DataPlaneError::NvmfTargetError(
                    "no NVMe-oF target available".to_string(),
                ));
            }
        };

        // Look up whether this subsystem was created with ANA enabled.
        let ana_enabled = sub_info.ana_group_id > 0;

        if ana_enabled {
            // spdk_nvmf_subsystem_set_ana_state requires a non-NULL trid to identify
            // which listener's ANA state to update. Build it from the cached address.
            let listen_addr = sub_info.listen_address.clone();
            let listen_port = sub_info.listen_port;
            let completion = Arc::new(Completion::new());
            let completion_clone = completion.clone();

            reactor_dispatch::send_to_reactor(move || unsafe {
                let tgt = tgt_ptr.as_ptr() as *mut ffi::spdk_nvmf_tgt;
                let subsystem =
                    ffi::spdk_nvmf_tgt_find_subsystem(tgt, nqn_c.as_ptr() as *const c_char);
                if subsystem.is_null() {
                    completion_clone.complete(());
                    return;
                }

                // Build trid matching the listener's transport ID.
                let mut trid: ffi::spdk_nvme_transport_id = std::mem::zeroed();
                trid.trtype = ffi::spdk_nvme_transport_type_SPDK_NVME_TRANSPORT_TCP;
                trid.adrfam = ffi::spdk_nvmf_adrfam_SPDK_NVMF_ADRFAM_IPV4;
                let addr_bytes = listen_addr.as_bytes();
                let port_str = format!("{}", listen_port);
                let port_bytes = port_str.as_bytes();
                std::ptr::copy_nonoverlapping(
                    addr_bytes.as_ptr() as *const c_char,
                    trid.traddr.as_mut_ptr(),
                    addr_bytes.len().min(trid.traddr.len()),
                );
                std::ptr::copy_nonoverlapping(
                    port_bytes.as_ptr() as *const c_char,
                    trid.trsvcid.as_mut_ptr(),
                    port_bytes.len().min(trid.trsvcid.len()),
                );

                let state = match ana_state_owned.as_str() {
                    "optimized" => ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_OPTIMIZED_STATE,
                    "non_optimized" | "non-optimized" => {
                        ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_NON_OPTIMIZED_STATE
                    }
                    "inaccessible" => ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_INACCESSIBLE_STATE,
                    _ => ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_OPTIMIZED_STATE,
                };
                let cb_arg = Arc::into_raw(completion_clone) as *mut c_void;
                ffi::spdk_nvmf_subsystem_set_ana_state(
                    subsystem,
                    &trid as *const ffi::spdk_nvme_transport_id,
                    state,
                    ana_group_id,
                    Some(ana_state_done_cb),
                    cb_arg,
                );
            });

            completion.wait();
        } else {
            error!(
                "ANA reporting not enabled on subsystem (ana_group_id=0), skipping set_ana_state"
            );
        }

        sub_info.ana_group_id = ana_group_id;
        sub_info.ana_state = ana_state.to_string();
        Ok(())
    }

    pub fn get_ana_state(&self, nqn: &str) -> Result<(u32, String)> {
        let subsystems = self.subsystems.lock().unwrap();
        let info = subsystems.get(nqn).ok_or_else(|| {
            DataPlaneError::NvmfTargetError(format!("subsystem not found: {}", nqn))
        })?;
        Ok((info.ana_group_id, info.ana_state.clone()))
    }
}

// ---------------------------------------------------------------------------
// SPDK FFI callbacks
// ---------------------------------------------------------------------------

/// Callback for `spdk_nvmf_subsystem_set_ana_state`. Signals the Completion
/// so the calling thread unblocks.
unsafe extern "C" fn ana_state_done_cb(cb_arg: *mut c_void, _status: i32) {
    let completion = Arc::from_raw(cb_arg as *const Completion<()>);
    completion.complete(());
}

/// Callback after `spdk_nvmf_subsystem_add_listener` completes.
/// On success, starts the subsystem. On failure, signals the completion.
unsafe extern "C" fn listener_done_cb(cb_arg: *mut c_void, status: i32) {
    let ctx_ptr = cb_arg;
    let ctx = &*(ctx_ptr as *const TargetCreateCtx);

    if status != 0 {
        error!("spdk_nvmf_subsystem_add_listener failed: rc={}", status);
        // Reclaim the ctx and signal failure.
        let ctx = Box::from_raw(ctx_ptr as *mut TargetCreateCtx);
        ctx.completion.complete(status);
        return;
    }

    // Chain: start the subsystem.
    let subsystem = ctx.subsystem.as_ptr() as *mut ffi::spdk_nvmf_subsystem;
    let rc = ffi::spdk_nvmf_subsystem_start(subsystem, Some(create_start_done_cb), ctx_ptr);
    if rc != 0 {
        // Synchronous failure — callback will NOT be invoked.
        error!("spdk_nvmf_subsystem_start synchronous failure: rc={}", rc);
        let ctx = Box::from_raw(ctx_ptr as *mut TargetCreateCtx);
        ctx.completion.complete(rc);
    }
}

/// Callback after `spdk_nvmf_subsystem_start` completes during create_target.
/// Signals the final completion with the result status.
unsafe extern "C" fn create_start_done_cb(
    _subsystem: *mut ffi::spdk_nvmf_subsystem,
    cb_arg: *mut c_void,
    status: i32,
) {
    let ctx = Box::from_raw(cb_arg as *mut TargetCreateCtx);
    if status != 0 {
        error!("spdk_nvmf_subsystem_start failed: rc={}", status);
    }
    ctx.completion.complete(status);
}

// ---------------------------------------------------------------------------
// SPDK FFI callbacks — delete target chain
// ---------------------------------------------------------------------------

/// EINPROGRESS errno value.  SPDK async operations return this when the
/// subsystem is still transitioning (e.g. connections draining).  The callback
/// will NOT be invoked — we must retry later.
const EINPROGRESS: i32 = -115;

/// Delay between EINPROGRESS retries (microseconds).
const DELETE_RETRY_US: u64 = 50_000; // 50ms

/// Try to stop the subsystem.  On EINPROGRESS, schedules a retry on the
/// reactor after a short delay instead of reporting a permanent failure.
unsafe fn retry_subsystem_stop(ctx_ptr: *mut c_void) {
    let dctx = &*(ctx_ptr as *const TargetDeleteCtx);
    let subsystem = dctx.subsystem.as_ptr() as *mut ffi::spdk_nvmf_subsystem;
    let rc = ffi::spdk_nvmf_subsystem_stop(subsystem, Some(delete_stop_done_cb), ctx_ptr);
    if rc == 0 {
        // Async stop in progress — callback will fire.
        return;
    }
    if rc == EINPROGRESS {
        warn!(
            "spdk_nvmf_subsystem_stop returned EINPROGRESS, retrying in {}us",
            DELETE_RETRY_US
        );
        schedule_retry_on_reactor(ctx_ptr, retry_subsystem_stop);
        return;
    }
    // True synchronous failure — callback will NOT be invoked.
    error!("spdk_nvmf_subsystem_stop synchronous failure: rc={}", rc);
    let dctx = Box::from_raw(ctx_ptr as *mut TargetDeleteCtx);
    dctx.completion.complete(rc);
}

/// Try to destroy the subsystem.  On EINPROGRESS, schedules a retry on the
/// reactor after a short delay.
unsafe fn retry_subsystem_destroy(ctx_ptr: *mut c_void) {
    let dctx = &*(ctx_ptr as *const TargetDeleteCtx);
    let subsystem = dctx.subsystem.as_ptr() as *mut ffi::spdk_nvmf_subsystem;
    let rc = ffi::spdk_nvmf_subsystem_destroy(subsystem, Some(delete_destroy_done_cb), ctx_ptr);
    if rc == 0 {
        // Async destroy in progress — callback will fire.
        return;
    }
    if rc == EINPROGRESS {
        warn!(
            "spdk_nvmf_subsystem_destroy returned EINPROGRESS, retrying in {}us",
            DELETE_RETRY_US
        );
        schedule_retry_on_reactor(ctx_ptr, retry_subsystem_destroy);
        return;
    }
    // True synchronous failure — callback will NOT be invoked.
    error!("spdk_nvmf_subsystem_destroy synchronous failure: rc={}", rc);
    let dctx = Box::from_raw(ctx_ptr as *mut TargetDeleteCtx);
    dctx.completion.complete(rc);
}

/// Schedule a retry function to run on the reactor after a short delay.
/// Uses `send_to_reactor` + `std::thread::sleep` in a background thread to
/// avoid blocking the reactor while waiting.
fn schedule_retry_on_reactor(ctx_ptr: *mut c_void, retry_fn: unsafe fn(*mut c_void)) {
    let ptr = ctx_ptr as usize;
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_micros(DELETE_RETRY_US));
        reactor_dispatch::send_to_reactor(move || unsafe {
            retry_fn(ptr as *mut c_void);
        });
    });
}

/// Callback after `spdk_nvmf_subsystem_stop` completes during delete_target.
/// On success, destroys the subsystem. On failure, signals the completion.
unsafe extern "C" fn delete_stop_done_cb(
    _subsystem: *mut ffi::spdk_nvmf_subsystem,
    cb_arg: *mut c_void,
    status: i32,
) {
    if status != 0 {
        error!("spdk_nvmf_subsystem_stop callback failed: rc={}", status);
        let ctx = Box::from_raw(cb_arg as *mut TargetDeleteCtx);
        ctx.completion.complete(status);
        return;
    }

    // Stop succeeded — now destroy.
    retry_subsystem_destroy(cb_arg);
}

/// Callback after `spdk_nvmf_subsystem_destroy` completes during delete_target.
/// Signals the final completion.
unsafe extern "C" fn delete_destroy_done_cb(cb_arg: *mut c_void) {
    let ctx = Box::from_raw(cb_arg as *mut TargetDeleteCtx);
    ctx.completion.complete(0);
}
