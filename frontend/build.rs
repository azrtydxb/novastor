fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/nvme.proto"], &["proto/"])
        .expect("failed to compile nvme.proto");
}
