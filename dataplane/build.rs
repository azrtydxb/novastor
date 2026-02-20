use std::env;
use std::path::PathBuf;

fn main() {
    let spdk_dir = env::var("SPDK_DIR").unwrap_or_else(|_| "/usr/local/lib/spdk".to_string());
    println!("cargo:rustc-link-search=native={}/lib", spdk_dir);
    println!("cargo:rerun-if-env-changed=SPDK_DIR");

    // Only link SPDK libraries when the spdk-sys feature is enabled
    if env::var("CARGO_FEATURE_SPDK_SYS").is_ok() {
        let spdk_libs = [
            "spdk_nvmf", "spdk_nvme", "spdk_bdev", "spdk_bdev_aio",
            "spdk_bdev_nvme", "spdk_bdev_malloc", "spdk_bdev_lvol",
            "spdk_blob", "spdk_blob_bdev", "spdk_lvol", "spdk_event",
            "spdk_event_bdev", "spdk_event_nvmf", "spdk_thread",
            "spdk_util", "spdk_env_dpdk", "spdk_json", "spdk_jsonrpc",
            "spdk_rpc", "spdk_log", "spdk_sock", "spdk_sock_posix",
            "spdk_trace",
        ];
        for lib in &spdk_libs {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
        let dpdk_libs = [
            "rte_eal", "rte_mempool", "rte_ring", "rte_mbuf",
            "rte_bus_pci", "rte_pci", "rte_vhost", "rte_telemetry", "rte_log",
        ];
        for lib in &dpdk_libs {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
        println!("cargo:rustc-link-lib=dylib=aio");
        println!("cargo:rustc-link-lib=dylib=uuid");
        println!("cargo:rustc-link-lib=dylib=numa");
        println!("cargo:rustc-link-lib=dylib=dl");
        println!("cargo:rustc-link-lib=dylib=rt");
        println!("cargo:rustc-link-lib=dylib=pthread");

        let spdk_include = format!("{}/include", spdk_dir);
        if PathBuf::from(&spdk_include).exists() {
            let bindings = bindgen::Builder::default()
                .header("src/spdk_wrapper.h")
                .clang_arg(format!("-I{}", spdk_include))
                .allowlist_function("spdk_.*")
                .allowlist_type("spdk_.*")
                .allowlist_var("SPDK_.*")
                .generate()
                .expect("Unable to generate SPDK bindings");
            let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
            bindings.write_to_file(out_path.join("spdk_bindings.rs")).expect("Couldn't write bindings");
        }
    }
}
