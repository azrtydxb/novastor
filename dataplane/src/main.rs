//! NovaStor SPDK Data Plane — binary entry point.

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "novastor-dataplane", version, about)]
struct Args {
    /// gRPC listen address for dataplane service
    #[arg(long, default_value = "/var/tmp/novastor-spdk.sock")]
    rpc_socket: String,

    /// SPDK reactor CPU mask (hex)
    #[arg(long, default_value = "0x1")]
    reactor_mask: String,

    /// Hugepage memory in MB
    #[arg(long, default_value_t = 2048)]
    mem_size: u32,

    /// NVMe-oF transport type
    #[arg(long, default_value = "TCP")]
    transport_type: String,

    /// NVMe-oF listen address
    #[arg(long, default_value = "0.0.0.0")]
    listen_address: String,

    /// NVMe-oF listen port
    #[arg(long, default_value_t = 4420)]
    listen_port: u16,

    /// gRPC inter-node listen port
    #[arg(long, default_value_t = 9500)]
    grpc_port: u16,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

fn main() {
    let args = Args::parse();

    // Build the tokio runtime early so that init_tracing can use it for the
    // OTLP batch exporter's background task.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    // Enter the runtime context so init_tracing can spawn the batch exporter.
    let _rt_guard = runtime.enter();
    novastor_dataplane::tracing_init::init_tracing("novastor-dataplane", &args.log_level);

    info!(
        "novastor-dataplane starting (reactor_mask={}, mem={}MB)",
        args.reactor_mask, args.mem_size
    );

    novastor_dataplane::set_tokio_handle(runtime.handle().clone());
    info!("tokio runtime started (2 worker threads)");

    #[cfg(feature = "spdk-sys")]
    {
        use log::error;
        use novastor_dataplane::config::DataPlaneConfig;

        let config = DataPlaneConfig {
            rpc_socket: args.rpc_socket,
            reactor_mask: args.reactor_mask,
            mem_size: args.mem_size,
            transport_type: args.transport_type,
            listen_address: args.listen_address,
            listen_port: args.listen_port,
            grpc_port: args.grpc_port,
        };

        // spdk::run() blocks in the SPDK reactor loop on the main thread.
        if let Err(e) = novastor_dataplane::spdk::run(config) {
            error!("data plane failed: {}", e);
            std::process::exit(1);
        }
    }

    #[cfg(not(feature = "spdk-sys"))]
    {
        info!("SPDK not available (spdk-sys feature not enabled). Exiting.");
        let _ = (
            args.rpc_socket,
            args.reactor_mask,
            args.mem_size,
            args.transport_type,
            args.listen_address,
            args.listen_port,
            args.grpc_port,
        );
    }

    // Bulk-destage all dirty bitmaps to redb (BlueStore V3 pattern).
    novastor_dataplane::chunk::sync::destage_all_bitmaps();

    // Flush pending OTel spans before tearing down the runtime.
    novastor_dataplane::tracing_init::shutdown_tracing();

    // Shut down tokio runtime after SPDK exits.
    runtime.shutdown_background();
    info!("novastor-dataplane stopped");
}
