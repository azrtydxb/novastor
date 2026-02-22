//! SPDK environment initialization and reactor management.

use crate::config::DataPlaneConfig;
use crate::error::Result;
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[cfg(feature = "spdk-sys")]
#[allow(non_camel_case_types, non_snake_case, non_upper_case_globals, dead_code, improper_ctypes)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/spdk_bindings.rs"));
}

/// Data passed to the SPDK startup callback via the arg pointer.
#[cfg(feature = "spdk-sys")]
struct SpdkStartupData {
    rpc_socket: String,
    listen_port: u16,
}

pub fn init_spdk_env(config: &DataPlaneConfig) -> Result<()> {
    info!(
        "SPDK env init: reactor_mask={}, mem={}MB",
        config.reactor_mask, config.mem_size
    );

    #[cfg(feature = "spdk-sys")]
    {
        use crate::error::DataPlaneError;

        unsafe {
            let rust_size = std::mem::size_of::<ffi::spdk_app_opts>();
            info!("sizeof(spdk_app_opts) in Rust = {} bytes (C expects 253)", rust_size);

            let mut opts: ffi::spdk_app_opts = std::mem::zeroed();
            ffi::spdk_app_opts_init(
                &mut opts,
                rust_size,
            );
            let app_name = std::ffi::CString::new("novastor-dataplane").unwrap();
            let reactor_mask = std::ffi::CString::new(config.reactor_mask.as_str()).unwrap();
            let hugedir = std::ffi::CString::new("/dev/hugepages").unwrap();
            opts.name = app_name.as_ptr();
            opts.reactor_mask = reactor_mask.as_ptr();
            opts.mem_size = config.mem_size as i32;
            opts.hugedir = hugedir.as_ptr();
            opts.rpc_addr = std::ptr::null();

            // Package config for the startup callback.  The callback runs
            // inside spdk_app_start *before* the reactor loop blocks, so the
            // JSON-RPC server is guaranteed to be listening before any agent
            // tries to connect.
            let startup_data = Box::new(SpdkStartupData {
                rpc_socket: config.rpc_socket.clone(),
                listen_port: config.listen_port,
            });
            let arg = Box::into_raw(startup_data) as *mut std::os::raw::c_void;

            let rc = ffi::spdk_app_start(&mut opts, Some(spdk_startup_cb), arg);
            if rc != 0 {
                return Err(DataPlaneError::SpdkInit(format!(
                    "spdk_app_start failed with rc={}", rc
                )));
            }
        }
    }

    #[cfg(not(feature = "spdk-sys"))]
    {
        info!("SPDK env init (stub mode — no SPDK libraries linked)");
    }

    Ok(())
}

#[cfg(feature = "spdk-sys")]
unsafe extern "C" fn spdk_startup_cb(arg: *mut std::os::raw::c_void) {
    use log::error;

    info!("SPDK startup callback: subsystems initialized");

    // Recover the startup data passed through the arg pointer.
    let data = Box::from_raw(arg as *mut SpdkStartupData);

    // Initialise managers and register RPC methods.
    let bdev_mgr = crate::spdk::bdev_manager::BdevManager::new();
    let nvmf_mgr = crate::spdk::nvmf_manager::NvmfManager::new(data.listen_port);
    crate::jsonrpc::methods::init_managers(bdev_mgr, nvmf_mgr);

    let mut router = crate::jsonrpc::server::Router::new();
    crate::jsonrpc::methods::register_all(&mut router);
    let router = Arc::new(router);

    info!("starting JSON-RPC server on {}", data.rpc_socket);
    match crate::jsonrpc::server::start_server(&data.rpc_socket, router) {
        Ok(_handle) => {
            // The JoinHandle is dropped here, which detaches the listener
            // thread.  It will run for the lifetime of the process, which
            // is controlled by the SPDK reactor loop.
            info!("JSON-RPC server started successfully");
        }
        Err(e) => {
            error!("failed to start JSON-RPC server: {}", e);
            // Cannot operate without the RPC server — stop SPDK.
            ffi::spdk_app_stop(-1);
        }
    }
}

pub fn run_reactor_loop() -> Result<()> {
    #[cfg(feature = "spdk-sys")]
    {
        // spdk_app_start blocks in the reactor loop until spdk_app_stop.
        Ok(())
    }

    #[cfg(not(feature = "spdk-sys"))]
    {
        info!("SPDK reactor loop (stub mode — waiting for shutdown signal)");
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();
        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .expect("failed to set ctrl-c handler");

        while running.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        info!("SPDK reactor loop exiting");
        Ok(())
    }
}

pub fn shutdown_spdk_env() {
    #[cfg(feature = "spdk-sys")]
    {
        unsafe {
            ffi::spdk_app_stop(0);
            ffi::spdk_app_fini();
        }
    }
    #[cfg(not(feature = "spdk-sys"))]
    {
        info!("SPDK env shutdown (stub mode)");
    }
}
