//! gRPC control plane for the frontend controller.
//!
//! Implements the NVMeTargetService so the CSI/agent can create and delete
//! volumes on this frontend node.

use log::info;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::volume_manager::VolumeManager;

pub mod nvme {
    tonic::include_proto!("nvme");
}

use nvme::nv_me_target_service_server::NvMeTargetService;
use nvme::{
    CreateTargetRequest, CreateTargetResponse, DeleteTargetRequest, DeleteTargetResponse,
    SetAnaStateRequest, SetAnaStateResponse,
};

pub struct FrontendTargetService {
    volume_manager: Arc<VolumeManager>,
    listen_addr: String,
    nvmeof_port: u16,
}

impl FrontendTargetService {
    pub fn new(
        volume_manager: Arc<VolumeManager>,
        listen_addr: String,
        nvmeof_port: u16,
    ) -> Self {
        Self {
            volume_manager,
            listen_addr,
            nvmeof_port,
        }
    }
}

#[tonic::async_trait]
impl NvMeTargetService for FrontendTargetService {
    async fn create_target(
        &self,
        request: Request<CreateTargetRequest>,
    ) -> Result<Response<CreateTargetResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id is required"));
        }
        if req.size_bytes <= 0 {
            return Err(Status::invalid_argument("size_bytes must be positive"));
        }

        info!(
            "CreateTarget: volume_id={}, size={}",
            req.volume_id, req.size_bytes
        );

        // The frontend creates a userspace NVMe-oF TCP target backed by NDP.
        let owner_addr = self
            .volume_manager
            .get_owner_addr(&req.volume_id)
            .await
            .map_err(|e| Status::internal(format!("no chunk engine available: {}", e)))?;

        let nqn = self
            .volume_manager
            .create_volume(&req.volume_id, req.size_bytes as u64, &owner_addr)
            .await
            .map_err(|e| Status::internal(e))?;

        Ok(Response::new(CreateTargetResponse {
            subsystem_nqn: nqn,
            target_address: self.listen_addr.clone(),
            target_port: self.nvmeof_port.to_string(),
        }))
    }

    async fn delete_target(
        &self,
        request: Request<DeleteTargetRequest>,
    ) -> Result<Response<DeleteTargetResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id is required"));
        }

        info!("DeleteTarget: volume_id={}", req.volume_id);

        self.volume_manager
            .delete_volume(&req.volume_id)
            .await
            .map_err(|e| Status::internal(e))?;

        Ok(Response::new(DeleteTargetResponse {}))
    }

    async fn set_ana_state(
        &self,
        _request: Request<SetAnaStateRequest>,
    ) -> Result<Response<SetAnaStateResponse>, Status> {
        // ANA state management is not needed for the kernel nvmet path —
        // kernel nvmet handles ANA natively via configfs.
        Ok(Response::new(SetAnaStateResponse {}))
    }
}
