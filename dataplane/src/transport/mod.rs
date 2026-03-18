//! Transport layer for inter-node communication.
//! gRPC for control plane, NDP for data plane.

pub mod chunk_client;
pub mod chunk_service;
pub mod dataplane_service;
pub mod ndp_server;
pub mod raft_service;
pub mod server;

pub mod chunk_proto {
    tonic::include_proto!("chunk");
}

pub mod dataplane_proto {
    tonic::include_proto!("dataplane");
}

pub mod raft_proto {
    tonic::include_proto!("novastor.raft");
}
