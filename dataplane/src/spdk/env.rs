//! SPDK environment initialization and reactor management.

use crate::config::DataPlaneConfig;
use crate::error::{DataPlaneError, Result};
use log::info;

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

/// Data passed to the SPDK startup callback via the arg pointer.
struct SpdkStartupData {
    listen_port: u16,
}

pub fn init_spdk_env(config: &DataPlaneConfig) -> Result<()> {
    info!(
        "SPDK env init: reactor_mask={}, mem={}MB",
        config.reactor_mask, config.mem_size
    );

    unsafe {
        let rust_size = std::mem::size_of::<ffi::spdk_app_opts>();
        info!("sizeof(spdk_app_opts) in Rust = {} bytes", rust_size);

        let mut opts: ffi::spdk_app_opts = std::mem::zeroed();
        ffi::spdk_app_opts_init(&mut opts, rust_size);

        let app_name = std::ffi::CString::new("novastor-dataplane").unwrap();
        let reactor_mask = std::ffi::CString::new(config.reactor_mask.as_str()).unwrap();
        let hugedir = std::ffi::CString::new("/dev/hugepages").unwrap();
        opts.name = app_name.as_ptr();
        opts.reactor_mask = reactor_mask.as_ptr();
        opts.mem_size = config.mem_size as i32;
        opts.hugedir = hugedir.as_ptr();
        // Enable SPDK's native RPC socket for SPDK-internal operations only.
        // This is used by the Rust dataplane to call SPDK subsystem methods
        // (e.g. bdev_nvme_attach_controller for NVMe-oF initiator). The Go
        // agent NEVER connects to this socket — all Go→Rust communication
        // uses gRPC exclusively (invariant #5).
        let rpc_sock = std::ffi::CString::new("/var/tmp/spdk.sock").unwrap();
        opts.rpc_addr = rpc_sock.as_ptr();

        // Right-size iobuf pools for NVMe-oF TCP transport.
        // NVMe-oF TCP needs ~383 large buffers; 512 gives headroom.
        // small=2048*8K=16MB, large=512*132K=66MB → ~82MB total.
        let mut iobuf_opts: ffi::spdk_iobuf_opts = std::mem::zeroed();
        ffi::spdk_iobuf_get_opts(&mut iobuf_opts, std::mem::size_of::<ffi::spdk_iobuf_opts>());
        iobuf_opts.small_pool_count = 2048;
        iobuf_opts.large_pool_count = 512;
        ffi::spdk_iobuf_set_opts(&iobuf_opts);
        info!(
            "iobuf pools: small={}x{}B, large={}x{}B",
            iobuf_opts.small_pool_count,
            iobuf_opts.small_bufsize,
            iobuf_opts.large_pool_count,
            iobuf_opts.large_bufsize
        );

        // Package config for the startup callback. The callback runs
        // inside spdk_app_start *before* the reactor loop blocks, so the
        // SPDK subsystems (bdev, nvmf) are guaranteed to be ready before
        // the gRPC server accepts connections.
        let startup_data = Box::new(SpdkStartupData {
            listen_port: config.listen_port,
        });
        let arg = Box::into_raw(startup_data) as *mut std::os::raw::c_void;

        let rc = ffi::spdk_app_start(&mut opts, Some(spdk_startup_cb), arg);
        if rc != 0 {
            return Err(DataPlaneError::SpdkInit(format!(
                "spdk_app_start failed with rc={}",
                rc
            )));
        }
    }

    Ok(())
}

unsafe extern "C" fn spdk_startup_cb(arg: *mut std::os::raw::c_void) {
    info!("SPDK startup callback: subsystems initialized");

    // Recover the startup data passed through the arg pointer.
    let _data = Box::from_raw(arg as *mut SpdkStartupData);

    // Initialise SPDK managers. These are used by the gRPC
    // DataplaneService to manage bdevs and NVMe-oF targets.
    // SPDK's native RPC socket is available for SPDK-internal operations
    // (e.g. bdev_nvme_attach_controller), but the Go agent communicates
    // exclusively via gRPC (invariant #5).
    info!("SPDK subsystems ready, awaiting gRPC connections");
}

pub fn shutdown_spdk_env() {
    unsafe {
        ffi::spdk_app_stop(0);
        ffi::spdk_app_fini();
    }
}
