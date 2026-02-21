//! SPDK environment initialization and reactor management.

use crate::config::DataPlaneConfig;
use crate::error::{DataPlaneError, Result};
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[cfg(feature = "spdk-sys")]
#[allow(non_camel_case_types, non_snake_case, non_upper_case_globals, dead_code, improper_ctypes)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/spdk_bindings.rs"));
}

pub fn init_spdk_env(config: &DataPlaneConfig) -> Result<()> {
    info!(
        "SPDK env init: reactor_mask={}, mem={}MB",
        config.reactor_mask, config.mem_size
    );

    #[cfg(feature = "spdk-sys")]
    {
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

            let rc = ffi::spdk_app_start(&mut opts, Some(spdk_startup_cb), std::ptr::null_mut());
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
unsafe extern "C" fn spdk_startup_cb(_arg: *mut std::os::raw::c_void) {
    info!("SPDK startup callback: subsystems initialized");
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
