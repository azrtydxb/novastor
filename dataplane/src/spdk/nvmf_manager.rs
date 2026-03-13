//! NVMe-oF target and initiator management.
//!
//! All SPDK NVMe-oF calls are dispatched to the reactor thread via
//! `reactor_dispatch::send_to_reactor` / `dispatch_sync`. Async SPDK
//! operations (add_transport, add_listener, start/stop/destroy subsystem)
//! use chained callbacks that signal a `Completion` when the full sequence
//! is done.

use crate::config::{NvmfInitiatorConfig, NvmfTargetConfig};
use crate::error::{DataPlaneError, Result};
use log::{error, info};
use std::collections::HashMap;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex};

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
        }
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

                    // Create the subsystem.
                    let subsystem = ffi::spdk_nvmf_subsystem_create(
                        tgt,
                        nqn_c.as_ptr() as *const c_char,
                        ffi::spdk_nvmf_subtype_SPDK_NVMF_SUBTYPE_NVME,
                        0, // num_ns — will add namespace separately
                    );
                    if subsystem.is_null() {
                        return Err(DataPlaneError::NvmfTargetError(format!(
                            "spdk_nvmf_subsystem_create failed for nqn={}",
                            nqn_c.to_str().unwrap_or("?")
                        )));
                    }

                    // Allow any host to connect.
                    ffi::spdk_nvmf_subsystem_set_allow_any_host(subsystem, true);

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
        // This must happen before spdk_nvmf_subsystem_add_listener.
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
            if rc != 0 {
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

        let info = SubsystemInfo {
            nqn: nqn.clone(),
            bdev_name: config.bdev_name.clone(),
            listen_address: config.listen_address.clone(),
            listen_port: config.listen_port,
            ana_group_id: config.ana_group_id,
            ana_state: config.ana_state.clone(),
        };
        self.subsystems.lock().unwrap().insert(nqn, info.clone());
        Ok(info)
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
            unsafe {
                let dctx = &*(ctx_ptr as *const TargetDeleteCtx);
                let subsystem = dctx.subsystem.as_ptr() as *mut ffi::spdk_nvmf_subsystem;
                ffi::spdk_nvmf_subsystem_stop(subsystem, Some(delete_stop_done_cb), ctx_ptr);
            }
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
            "connecting NVMe-oF initiator: nqn={}, remote={}:{}",
            config.nqn, config.remote_address, config.remote_port
        );

        let addr_c = std::ffi::CString::new(config.remote_address.as_str())
            .map_err(|e| DataPlaneError::NvmfInitiatorError(format!("invalid addr: {e}")))?;
        let port_str = config.remote_port.to_string();
        let port_c = std::ffi::CString::new(port_str.as_str()).unwrap();
        let nqn_c = std::ffi::CString::new(config.nqn.as_str())
            .map_err(|e| DataPlaneError::NvmfInitiatorError(format!("invalid NQN: {e}")))?;

        reactor_dispatch::dispatch_sync(move || -> Result<()> {
            unsafe {
                // Build the transport ID for the remote target.
                let mut trid: ffi::spdk_nvme_transport_id = std::mem::zeroed();
                trid.trtype = ffi::spdk_nvme_transport_type_SPDK_NVME_TRANSPORT_TCP;
                trid.adrfam = ffi::spdk_nvmf_adrfam_SPDK_NVMF_ADRFAM_IPV4;

                let addr_bytes = addr_c.as_bytes_with_nul();
                let port_bytes = port_c.as_bytes_with_nul();
                let nqn_bytes = nqn_c.as_bytes_with_nul();

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
                std::ptr::copy_nonoverlapping(
                    nqn_bytes.as_ptr() as *const c_char,
                    trid.subnqn.as_mut_ptr(),
                    nqn_bytes.len().min(trid.subnqn.len()),
                );

                // Probe and attach the remote NVMe controller.
                let mut opts: ffi::spdk_nvme_ctrlr_opts = std::mem::zeroed();
                ffi::spdk_nvme_ctrlr_get_default_ctrlr_opts(
                    &mut opts,
                    std::mem::size_of_val(&opts),
                );

                let ctrlr = ffi::spdk_nvme_connect(&trid, &opts, std::mem::size_of_val(&opts));
                if ctrlr.is_null() {
                    return Err(DataPlaneError::NvmfInitiatorError(
                        "spdk_nvme_connect failed".to_string(),
                    ));
                }
                Ok(())
            }
        })?;

        let info = InitiatorInfo {
            nqn: config.nqn.clone(),
            remote_address: config.remote_address.clone(),
            remote_port: config.remote_port,
            local_bdev_name: config.bdev_name.clone(),
        };
        self.initiators
            .lock()
            .unwrap()
            .insert(config.bdev_name.clone(), info.clone());
        Ok(info)
    }

    pub fn disconnect_initiator(&self, bdev_name: &str) -> Result<()> {
        info!("disconnecting NVMe-oF initiator: bdev={}", bdev_name);

        let initiator = {
            let initiators = self.initiators.lock().unwrap();
            initiators.get(bdev_name).cloned()
        };
        let initiator = initiator.ok_or_else(|| {
            DataPlaneError::NvmfInitiatorError(format!("initiator {} not found", bdev_name))
        })?;

        let nqn_c = std::ffi::CString::new(initiator.nqn.as_str())
            .map_err(|e| DataPlaneError::NvmfInitiatorError(format!("invalid NQN: {e}")))?;

        reactor_dispatch::dispatch_sync(move || -> Result<()> {
            unsafe {
                let mut trid: ffi::spdk_nvme_transport_id = std::mem::zeroed();
                trid.trtype = ffi::spdk_nvme_transport_type_SPDK_NVME_TRANSPORT_TCP;

                let nqn_bytes = nqn_c.as_bytes_with_nul();
                std::ptr::copy_nonoverlapping(
                    nqn_bytes.as_ptr() as *const c_char,
                    trid.subnqn.as_mut_ptr(),
                    nqn_bytes.len().min(trid.subnqn.len()),
                );

                let ctrlr = ffi::spdk_nvme_connect(&trid, std::ptr::null(), 0);
                if !ctrlr.is_null() {
                    let rc = ffi::spdk_nvme_detach(ctrlr);
                    if rc != 0 {
                        return Err(DataPlaneError::NvmfInitiatorError(format!(
                            "spdk_nvme_detach failed: rc={rc}"
                        )));
                    }
                }
                Ok(())
            }
        })?;

        self.initiators.lock().unwrap().remove(bdev_name);
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

        reactor_dispatch::dispatch_sync(move || unsafe {
            let tgt = tgt_ptr.as_ptr() as *mut ffi::spdk_nvmf_tgt;
            let subsystem = ffi::spdk_nvmf_tgt_find_subsystem(tgt, nqn_c.as_ptr() as *const c_char);
            if !subsystem.is_null() {
                let state = match ana_state_owned.as_str() {
                    "optimized" => ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_OPTIMIZED_STATE,
                    "non_optimized" => ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_NON_OPTIMIZED_STATE,
                    "inaccessible" => ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_INACCESSIBLE_STATE,
                    _ => ffi::spdk_nvme_ana_state_SPDK_NVME_ANA_OPTIMIZED_STATE,
                };
                ffi::spdk_nvmf_subsystem_set_ana_state(
                    subsystem,
                    std::ptr::null(),
                    state,
                    ana_group_id,
                    None,
                    std::ptr::null_mut(),
                );
            }
        });

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
// SPDK FFI callbacks — create target chain
// ---------------------------------------------------------------------------

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
    ffi::spdk_nvmf_subsystem_start(subsystem, Some(create_start_done_cb), ctx_ptr);
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

/// Callback after `spdk_nvmf_subsystem_stop` completes during delete_target.
/// On success, destroys the subsystem. On failure, signals the completion.
unsafe extern "C" fn delete_stop_done_cb(
    _subsystem: *mut ffi::spdk_nvmf_subsystem,
    cb_arg: *mut c_void,
    status: i32,
) {
    let ctx_ptr = cb_arg;
    let ctx = &*(ctx_ptr as *const TargetDeleteCtx);

    if status != 0 {
        error!("spdk_nvmf_subsystem_stop failed: rc={}", status);
        let ctx = Box::from_raw(ctx_ptr as *mut TargetDeleteCtx);
        ctx.completion.complete(status);
        return;
    }

    let subsystem = ctx.subsystem.as_ptr() as *mut ffi::spdk_nvmf_subsystem;
    ffi::spdk_nvmf_subsystem_destroy(subsystem, Some(delete_destroy_done_cb), ctx_ptr);
}

/// Callback after `spdk_nvmf_subsystem_destroy` completes during delete_target.
/// Signals the final completion.
unsafe extern "C" fn delete_destroy_done_cb(cb_arg: *mut c_void) {
    let ctx = Box::from_raw(cb_arg as *mut TargetDeleteCtx);
    ctx.completion.complete(0);
}
