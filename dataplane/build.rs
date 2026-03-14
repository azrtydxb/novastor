use std::env;
use std::path::PathBuf;

fn main() {
    // Compile gRPC proto files for tonic.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/chunk_service.proto", "proto/raft_service.proto"],
            &["proto/"],
        )
        .expect("failed to compile proto files");

    // Only compile SPDK/uring C code when the spdk-sys feature is enabled.
    // Without this gate, `cargo test` on a machine without SPDK headers fails.
    // We still emit an empty bindings stub so that the include!() call in
    // backend/lvm.rs resolves without SPDK headers present.
    if env::var("CARGO_FEATURE_SPDK_SYS").is_err() {
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        std::fs::write(
            out_path.join("spdk_bindings.rs"),
            b"// stub: spdk-sys feature not enabled\n",
        )
        .expect("write stub bindings");
        return;
    }

    let spdk_dir = env::var("SPDK_DIR").unwrap_or_else(|_| "/usr/local".to_string());
    println!("cargo:rustc-link-search=native={}/lib", spdk_dir);
    println!("cargo:rerun-if-env-changed=SPDK_DIR");

    let lib_dir_path = format!("{}/lib", spdk_dir);

    // Collect all SPDK, DPDK, and ISA-L static library paths.
    let mut static_libs: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&lib_dir_path) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if (name.starts_with("libspdk_") || name.starts_with("librte_") || name.starts_with("libisal"))
                && name.ends_with(".a")
                // Exclude iSCSI target — we only use NVMe-oF.  The iSCSI
                // subsystem's PDU pool allocates ~300MB of hugepage memory
                // on init, which is wasteful for our use case.
                && !matches!(name.as_str(),
                    "libspdk_iscsi.a" | "libspdk_event_iscsi.a"
                )
            {
                static_libs.push(entry.path().to_string_lossy().to_string());
            }
        }
    }
    static_libs.sort();

    // Use --whole-archive inside --start-group/--end-group to resolve
    // all circular dependencies between SPDK and DPDK static archives.
    // --whole-archive forces the linker to include every object from each
    // archive, which is critical because both SPDK and DPDK rely on
    // constructor functions (__attribute__((constructor))) for self-
    // registration of subsystems and drivers (e.g. the mempool ring
    // driver in librte_mempool_ring.a).  Without --whole-archive the
    // linker drops these unreferenced constructors and the drivers
    // never register, causing runtime failures like
    // "spdk_event_mempool creation failed".
    println!("cargo:rustc-link-arg=-Wl,--whole-archive");
    println!("cargo:rustc-link-arg=-Wl,--start-group");
    for lib_path in &static_libs {
        println!("cargo:rustc-link-arg={}", lib_path);
    }
    println!("cargo:rustc-link-arg=-Wl,--end-group");
    println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");

    // System dynamic libraries placed after the static archive group so
    // that symbols referenced by SPDK objects (e.g. __stack_chk_guard from
    // glibc) can be resolved.
    for lib in &[
        "c", "m", "dl", "rt", "pthread", "aio", "uuid", "numa", "ssl", "crypto", "uring", "fuse3",
        "json-c", "gcc_s",
    ] {
        println!("cargo:rustc-link-arg=-l{}", lib);
    }

    // Add multiarch and GCC library search paths for system libs.
    println!("cargo:rustc-link-search=native=/usr/lib/aarch64-linux-gnu");
    println!("cargo:rustc-link-search=native=/usr/lib/x86_64-linux-gnu");
    // GCC runtime libs (libatomic, libgcc_s) on Debian bookworm
    for ver in &["12", "13", "14"] {
        println!(
            "cargo:rustc-link-search=native=/usr/lib/gcc/aarch64-linux-gnu/{}",
            ver
        );
        println!(
            "cargo:rustc-link-search=native=/usr/lib/gcc/x86_64-linux-gnu/{}",
            ver
        );
    }

    // Compile the C uring wrapper.
    let spdk_include = format!("{}/include", spdk_dir);
    cc::Build::new()
        .file("src/uring_wrapper.c")
        .include(&spdk_include)
        .compile("uring_wrapper");

    let spdk_include = format!("{}/include", spdk_dir);
    if PathBuf::from(&spdk_include).exists() {
        let bindings = bindgen::Builder::default()
            .header("src/spdk_wrapper.h")
            .clang_arg(format!("-I{}", spdk_include))
            .allowlist_function("spdk_app_.*")
            .allowlist_function("spdk_bdev_.*")
            .allowlist_function("spdk_nvmf_.*")
            .allowlist_function("spdk_nvme_transport_id.*")
            .allowlist_function("spdk_json.*")
            .allowlist_function("spdk_rpc.*")
            .allowlist_function("spdk_log.*")
            .allowlist_function("spdk_thread.*")
            .allowlist_function("spdk_lvol.*")
            .allowlist_function("spdk_lvs.*")
            .allowlist_type("spdk_lvs_opts")
            .allowlist_function("spdk_blob.*")
            .allowlist_function("spdk_dma_.*")
            .allowlist_function("spdk_io_device_register")
            .allowlist_function("spdk_io_device_unregister")
            .allowlist_function("spdk_get_io_channel")
            .allowlist_function("spdk_put_io_channel")
            .allowlist_function("spdk_nvme_.*")
            .allowlist_function("create_aio_bdev")
            .allowlist_function("novastor_create_uring_bdev")
            .allowlist_function("bdev_aio_delete")
            .allowlist_function("create_malloc_disk")
            .allowlist_function("delete_malloc_disk")
            .allowlist_function("spdk_bdev_create_bs_dev_ext")
            .allowlist_function("spdk_bs_total_data_cluster_count")
            .allowlist_function("spdk_bs_free_cluster_count")
            .allowlist_function("spdk_bs_get_cluster_size")
            .allowlist_type("malloc_bdev_opts")
            .allowlist_type("spdk_nvme_ctrlr_opts")
            .allowlist_type("spdk_nvme_ctrlr")
            .allowlist_type("spdk_app_opts")
            .allowlist_type("spdk_bdev")
            .allowlist_type("spdk_bdev_io")
            .allowlist_type("spdk_bdev_desc")
            .allowlist_type("spdk_lvol")
            .allowlist_function("spdk_bdev_desc_get_bdev")
            .allowlist_function("spdk_bdev_get_name")
            .allowlist_function("spdk_bdev_first")
            .allowlist_function("spdk_bdev_next")
            .allowlist_var("LVS_CLEAR_.*")
            .allowlist_var("LVOL_CLEAR_.*")
            .allowlist_type("lvs_clear_method")
            .allowlist_type("lvol_clear_method")
            .allowlist_function("vbdev_lvs_create")
            .allowlist_function("vbdev_lvol_create")
            .allowlist_function("vbdev_lvol_create_snapshot")
            .allowlist_function("vbdev_lvol_create_clone")
            .allowlist_function("vbdev_lvol_resize")
            .allowlist_function("vbdev_lvol_destroy")
            .allowlist_function("spdk_bdev_get_module_ctx")
            .allowlist_function("spdk_iobuf_set_opts")
            .allowlist_function("spdk_iobuf_get_opts")
            .allowlist_type("spdk_iobuf_opts")
            .allowlist_function("spdk_thread_send_msg")
            .allowlist_function("spdk_thread_get_app_thread")
            .allowlist_function("spdk_get_thread")
            .allowlist_type("spdk_nvmf_target")
            .allowlist_type("spdk_nvmf_subsystem")
            .allowlist_type("spdk_nvmf_transport_id")
            .allowlist_var("SPDK_.*")
            .layout_tests(false)
            .derive_default(true)
            .derive_copy(false)
            .disable_header_comment()
            .raw_line("#[allow(non_camel_case_types, non_snake_case, non_upper_case_globals, dead_code, improper_ctypes)]")
            .raw_line("")
            .opaque_type("spdk_nvme_ctrlr_data")
            .opaque_type("spdk_nvmf_fabric_connect_rsp")
            .opaque_type("spdk_nvmf_fabric_prop_get_rsp")
            .opaque_type("spdk_bdev_ext_io_opts")
            .opaque_type("spdk_nvme_tcp_cmd")
            .opaque_type("spdk_nvme_tcp_rsp")
            .opaque_type("spdk_nvmf_transport_opts")
            .opaque_type("spdk_nvmf_ctrlr_feat")
            .opaque_type("spdk_nvmf_ctrlr_migr_data")
            .opaque_type("nvmf_h2c_msg")
            .opaque_type("spdk_nvme_cmd")
            .opaque_type("spdk_bdev_io_nvme_passthru_params")
            .generate()
            .expect("Unable to generate SPDK bindings");
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        let bindings_str = bindings.to_string();
        // Strip inner attributes (#![...]) which are invalid inside include!() in a module.
        let filtered: String = bindings_str
            .lines()
            .filter(|line| !line.trim_start().starts_with("#!["))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(out_path.join("spdk_bindings.rs"), filtered)
            .expect("Couldn't write bindings");
    }
}
