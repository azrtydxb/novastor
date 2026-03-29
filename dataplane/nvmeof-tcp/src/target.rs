//! NVMe-oF TCP target — multi-subsystem userspace NVMe-oF TCP server.
//!
//! One TCP listener serves all volumes. Each volume is a subsystem identified
//! by its NQN. The initiator specifies which subsystem to connect to via the
//! Fabric Connect command.

use async_trait::async_trait;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::error::{NvmeOfError, Result};
use crate::pdu::*;

/// Backend trait for NVMe-oF target storage.
#[async_trait]
pub trait NvmeOfBackend: Send + Sync + 'static {
    fn size(&self) -> u64;
    fn block_size(&self) -> u32 {
        512
    }
    async fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>>;
    async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
    async fn write_zeroes(&self, offset: u64, length: u32) -> Result<()> {
        let _ = (offset, length);
        Ok(())
    }
    async fn unmap(&self, ranges: &[(u64, u32)]) -> Result<()> {
        let _ = ranges;
        Ok(())
    }
}

/// Configuration for an NVMe-oF subsystem.
pub struct SubsystemConfig {
    pub nqn: String,
    pub serial: String,
    pub model: String,
}

/// A registered subsystem with its backend.
struct Subsystem {
    config: SubsystemConfig,
    backend: Box<dyn NvmeOfBackend>,
    /// Unique controller ID counter — each connection gets a different cntlid.
    /// Required for NVMe multipath: kernel rejects duplicate cntlid on same NQN.
    next_cntlid: AtomicU16,
}

/// Multi-subsystem NVMe-oF TCP target.
/// One listener, many volumes — connections are routed by NQN.
pub struct NvmeOfTarget {
    subsystems: Arc<RwLock<HashMap<String, Arc<Subsystem>>>>,
}

impl NvmeOfTarget {
    pub fn new() -> Self {
        Self {
            subsystems: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a subsystem (volume) with the target.
    pub fn add_subsystem<B: NvmeOfBackend>(&self, config: SubsystemConfig, backend: B) {
        let nqn = config.nqn.clone();
        let subsystem = Arc::new(Subsystem {
            config,
            backend: Box::new(backend),
            // Use hostname for per-node seed — unique per container/node.
            // /etc/machine-id doesn't exist in containers, PID=1 for all.
            next_cntlid: {
                static NODE_SEED: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
                let seed = *NODE_SEED.get_or_init(|| {
                    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
                        .unwrap_or_else(|_| format!("{}", std::process::id()));
                    let mut h: u64 = 0xcbf29ce484222325;
                    for b in hostname.as_bytes() {
                        h ^= *b as u64;
                        h = h.wrapping_mul(0x100000001b3);
                    }
                    ((h % 60000) as u16).max(1)
                });
                static SUBSYS_COUNTER: AtomicU16 = AtomicU16::new(0);
                let offset = SUBSYS_COUNTER.fetch_add(8, Ordering::Relaxed); // 8 connections per subsystem
                AtomicU16::new(seed.wrapping_add(offset))
            },
        });
        // Use try_write to avoid blocking; in practice this is called from async context.
        if let Ok(mut subs) = self.subsystems.try_write() {
            subs.insert(nqn.clone(), subsystem);
            info!("NVMe-oF: registered subsystem {}", nqn);
        } else {
            // Fallback: spawn a task to write.
            let subs = self.subsystems.clone();
            let nqn2 = nqn.clone();
            tokio::spawn(async move {
                subs.write().await.insert(nqn2.clone(), subsystem);
                info!("NVMe-oF: registered subsystem {}", nqn2);
            });
        }
    }

    /// Remove a subsystem.
    pub fn remove_subsystem(&self, nqn: &str) {
        if let Ok(mut subs) = self.subsystems.try_write() {
            subs.remove(nqn);
            info!("NVMe-oF: removed subsystem {}", nqn);
        }
    }

    /// Look up a subsystem by NQN.
    async fn get_subsystem(&self, nqn: &str) -> Option<Arc<Subsystem>> {
        self.subsystems.read().await.get(nqn).cloned()
    }

    /// List all registered subsystem NQNs.
    async fn list_subsystems(&self) -> Vec<String> {
        self.subsystems.read().await.keys().cloned().collect()
    }

    /// Start serving on the given TCP listener.
    pub async fn serve(self: &Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    info!("NVMe-oF: connection from {}", peer);
                    stream.set_nodelay(true).ok();
                    let target = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = target.handle_connection(stream).await {
                            match e {
                                NvmeOfError::ConnectionClosed => {
                                    debug!("NVMe-oF: connection closed from {}", peer);
                                }
                                _ => {
                                    error!("NVMe-oF: error from {}: {}", peer, e);
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("NVMe-oF: accept error: {}", e);
                }
            }
        }
    }

    async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        // ICReq/ICResp exchange.
        // First peek at the initial bytes to debug protocol.
        let mut ic_buf = [0u8; IC_REQ_SIZE];
        stream.read_exact(&mut ic_buf).await?;
        info!("NVMe-oF: ICReq first 16 bytes: {:02X?}", &ic_buf[..16]);
        let _ic_req = IcReq::decode(&ic_buf)?;

        // Echo back the initiator's digest request. If the initiator requests
        // digests and we don't echo them, the kernel aborts the connection.
        // For now we accept the request but don't actually compute digests
        // (the kernel will still work if we don't verify incoming digests).
        let ic_resp = IcResp {
            pfv: 0,
            cpda: 0,
            dgst: _ic_req.dgst, // Echo initiator's digest flags
            maxdata: 131072,    // 128KB max H2C data capsule
        };
        stream.write_all(&ic_resp.encode()).await?;
        stream.flush().await?;

        info!(
            "NVMe-oF: IC handshake complete (dgst=0x{:02X})",
            _ic_req.dgst
        );

        // Split stream for concurrent I/O. Responses go through a channel
        // so the writer task can batch them (fewer syscalls).
        let (reader, mut writer) = tokio::io::split(stream);
        let mut reader = reader;
        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);

        // Writer task: drains response channel and writes to stream.
        let writer_handle = tokio::spawn(async move {
            while let Some(data) = resp_rx.recv().await {
                if writer.write_all(&data).await.is_err() {
                    break;
                }
                // Batch: drain any pending responses before flushing
                while let Ok(more) = resp_rx.try_recv() {
                    if writer.write_all(&more).await.is_err() {
                        break;
                    }
                }
                if writer.flush().await.is_err() {
                    break;
                }
            }
        });

        // Wrap resp_tx in Arc for sharing with spawned tasks
        let resp_tx = Arc::new(resp_tx);
        let mut state = ConnectionState::new();

        loop {
            let mut hdr_buf = [0u8; PDU_HEADER_SIZE];
            match reader.read_exact(&mut hdr_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(NvmeOfError::ConnectionClosed);
                }
                Err(e) => return Err(e.into()),
            }
            let hdr = PduHeader::decode(&hdr_buf);

            match hdr.pdu_type {
                PDU_TYPE_CAPSULE_CMD => {
                    // Read the full capsule from the reader (must be done inline
                    // to keep the stream in sync), then dispatch.
                    let mut cmd_buf = [0u8; NVME_CMD_SIZE];
                    reader.read_exact(&mut cmd_buf).await?;
                    let cmd = NvmeCmd::decode(&cmd_buf);

                    let hdr_remaining =
                        (hdr.hlen as usize).saturating_sub(PDU_HEADER_SIZE + NVME_CMD_SIZE);
                    if hdr_remaining > 0 {
                        let mut pad = vec![0u8; hdr_remaining];
                        reader.read_exact(&mut pad).await?;
                    }

                    let data_len = (hdr.plen as usize).saturating_sub(hdr.hlen as usize);
                    let inline_data = if data_len > 0 && data_len <= 1_048_576 {
                        let pdo_skip = if hdr.pdo > 0 {
                            (hdr.pdo as usize)
                                .saturating_sub(hdr.hlen as usize)
                                .min(data_len)
                        } else {
                            0
                        };
                        if pdo_skip > 0 {
                            let mut pad = vec![0u8; pdo_skip];
                            reader.read_exact(&mut pad).await?;
                        }
                        let actual = data_len.saturating_sub(pdo_skip);
                        if actual > 0 {
                            let mut data = vec![0u8; actual];
                            reader.read_exact(&mut data).await?;
                            Some(data)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Fabric/Admin commands: process inline (fast, no NDP).
                    // I/O commands: spawn concurrent task.
                    let fctype = (cmd.nsid & 0xFF) as u8;
                    if cmd.opcode == opcode::FABRIC || state.sq_id == 0 {
                        // Admin/Fabric — write response directly via channel.
                        // We create a ChannelWriter that buffers writes and sends via channel.
                        let mut cw = ChannelWriter::new(resp_tx.clone());
                        self.handle_admin_or_fabric(&mut cw, &cmd, &mut state, inline_data)
                            .await?;
                        cw.flush_to_channel().await;
                    } else {
                        // I/O command — spawn concurrent task
                        let subsys = state.subsystem.clone();
                        let sq_id = state.sq_id;

                        // For writes without inline data, register pending write
                        if cmd.opcode == opcode::WRITE
                            && inline_data.is_none()
                            && !state.pending_write_data.contains_key(&cmd.cid)
                        {
                            let block_size = subsys
                                .as_ref()
                                .map(|s| s.backend.block_size())
                                .unwrap_or(512) as u64;
                            let lba = ((cmd.cdw11 as u64) << 32) | cmd.cdw10 as u64;
                            let nlb = (cmd.cdw12 & 0xFFFF) as u32 + 1;
                            let offset = lba * block_size;
                            let expected = (nlb * block_size as u32) as usize;

                            // Send R2T via response channel
                            let r2t = build_r2t(cmd.cid, state.next_ttag(), 0, expected as u32);
                            let _ = resp_tx.send(r2t.to_vec()).await;

                            if let Some(s) = subsys.as_ref() {
                                state.pending_writes.insert(
                                    cmd.cid,
                                    PendingWrite {
                                        subsystem: s.clone(),
                                        offset,
                                        expected,
                                        sq_id,
                                    },
                                );
                            }
                            continue;
                        }

                        // For writes with inline data or reads — spawn task
                        let write_data =
                            inline_data.or_else(|| state.pending_write_data.remove(&cmd.cid));
                        let tx = resp_tx.clone();

                        tokio::spawn(async move {
                            let subsys = match subsys {
                                Some(s) => s,
                                None => return,
                            };
                            match cmd.opcode {
                                opcode::READ => {
                                    let bs = subsys.backend.block_size() as u64;
                                    let lba = ((cmd.cdw11 as u64) << 32) | cmd.cdw10 as u64;
                                    let nlb = (cmd.cdw12 & 0xFFFF) as u32 + 1;
                                    match subsys.backend.read(lba * bs, nlb * bs as u32).await {
                                        Ok(data) => {
                                            let c2h = build_c2h_data_header(
                                                cmd.cid,
                                                0,
                                                data.len() as u32,
                                                true,
                                            );
                                            let mut resp =
                                                Vec::with_capacity(c2h.len() + data.len());
                                            resp.extend_from_slice(&c2h);
                                            resp.extend_from_slice(&data);
                                            let _ = tx.send(resp).await;
                                        }
                                        Err(e) => {
                                            warn!("NVMe-oF: read error: {}", e);
                                            let _ = tx
                                                .send(
                                                    build_capsule_resp(&NvmeCqe::error(
                                                        cmd.cid,
                                                        sq_id,
                                                        status::SCT_GENERIC,
                                                        status::DATA_XFER_ERROR,
                                                    ))
                                                    .to_vec(),
                                                )
                                                .await;
                                        }
                                    }
                                }
                                opcode::WRITE => {
                                    let bs = subsys.backend.block_size() as u64;
                                    let lba = ((cmd.cdw11 as u64) << 32) | cmd.cdw10 as u64;
                                    let nlb = (cmd.cdw12 & 0xFFFF) as u32 + 1;
                                    let expected = (nlb * bs as u32) as usize;
                                    let resp = if let Some(data) = write_data {
                                        let len = expected.min(data.len());
                                        match subsys.backend.write(lba * bs, &data[..len]).await {
                                            Ok(()) => build_capsule_resp(&NvmeCqe::success(
                                                cmd.cid, sq_id,
                                            )),
                                            Err(e) => {
                                                warn!("NVMe-oF: write error: {}", e);
                                                build_capsule_resp(&NvmeCqe::error(
                                                    cmd.cid,
                                                    sq_id,
                                                    status::SCT_GENERIC,
                                                    status::DATA_XFER_ERROR,
                                                ))
                                            }
                                        }
                                    } else {
                                        build_capsule_resp(&NvmeCqe::error(
                                            cmd.cid,
                                            sq_id,
                                            status::SCT_GENERIC,
                                            status::DATA_XFER_ERROR,
                                        ))
                                    };
                                    let _ = tx.send(resp.to_vec()).await;
                                }
                                opcode::FLUSH => {
                                    let _ = subsys.backend.flush().await;
                                    let _ = tx
                                        .send(
                                            build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id))
                                                .to_vec(),
                                        )
                                        .await;
                                }
                                opcode::WRITE_ZEROES => {
                                    let bs = subsys.backend.block_size() as u64;
                                    let lba = ((cmd.cdw11 as u64) << 32) | cmd.cdw10 as u64;
                                    let nlb = (cmd.cdw12 & 0xFFFF) as u32 + 1;
                                    let resp = match subsys
                                        .backend
                                        .write_zeroes(lba * bs, nlb * bs as u32)
                                        .await
                                    {
                                        Ok(()) => {
                                            build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id))
                                        }
                                        Err(e) => {
                                            warn!("NVMe-oF: write_zeroes error: {}", e);
                                            build_capsule_resp(&NvmeCqe::error(
                                                cmd.cid,
                                                sq_id,
                                                status::SCT_GENERIC,
                                                status::DATA_XFER_ERROR,
                                            ))
                                        }
                                    };
                                    let _ = tx.send(resp.to_vec()).await;
                                }
                                opcode::DATASET_MGMT => {
                                    let nr = (cmd.cdw10 & 0xFF) as usize + 1;
                                    let ad = (cmd.cdw11 >> 2) & 1;
                                    if ad == 1 {
                                        if let Some(data) = write_data {
                                            let bs = subsys.backend.block_size() as u64;
                                            let dsm_ranges =
                                                crate::pdu::DsmRange::parse_ranges(&data, nr);
                                            let ranges: Vec<(u64, u32)> = dsm_ranges
                                                .iter()
                                                .filter(|r| r.length_blocks > 0)
                                                .map(|r| {
                                                    (
                                                        r.starting_lba * bs,
                                                        (r.length_blocks as u64 * bs) as u32,
                                                    )
                                                })
                                                .collect();
                                            if !ranges.is_empty() {
                                                if let Err(e) = subsys.backend.unmap(&ranges).await
                                                {
                                                    warn!("NVMe-oF: unmap error: {}", e);
                                                }
                                            }
                                        }
                                    }
                                    let _ = tx
                                        .send(
                                            build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id))
                                                .to_vec(),
                                        )
                                        .await;
                                }
                                opcode::COMPARE => {
                                    // Compare: read data from backend and compare with
                                    // provided data. Return success if equal, error if not.
                                    let offset = cmd.slba * subsys.backend.block_size() as u64;
                                    let nlb = (cmd.cdw12 & 0xFFFF) as u32 + 1;
                                    let length = nlb as u64 * subsys.backend.block_size() as u64;
                                    let resp = match subsys.backend.read(offset, length as u32).await {
                                        Ok(backend_data) => {
                                            if let Some(ref cmp_data) = write_data {
                                                if backend_data.len() == cmp_data.len()
                                                    && backend_data == cmp_data.as_slice()
                                                {
                                                    build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id))
                                                } else {
                                                    // Compare failure: status COMPARE_FAILURE (0x285)
                                                    build_capsule_resp(&NvmeCqe::error(
                                                        cmd.cid, sq_id, 0x02, 0x85,
                                                    ))
                                                }
                                            } else {
                                                build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id))
                                            }
                                        }
                                        Err(_) => build_capsule_resp(&NvmeCqe::error(
                                            cmd.cid, sq_id, status::SCT_GENERIC, 0x04,
                                        )),
                                    };
                                    let _ = tx.send(resp.to_vec()).await;
                                }
                                opcode::WRITE_UNCORRECTABLE => {
                                    // Write Uncorrectable: marks LBAs as unreadable.
                                    // No-op for virtual storage — we don't track
                                    // uncorrectable blocks.
                                    let _ = tx
                                        .send(
                                            build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id))
                                                .to_vec(),
                                        )
                                        .await;
                                }
                                opcode::RESERVATION_REGISTER
                                | opcode::RESERVATION_REPORT
                                | opcode::RESERVATION_ACQUIRE
                                | opcode::RESERVATION_RELEASE => {
                                    // Reservations: not supported.
                                    // Return INVALID_OPCODE — this is correct for
                                    // unsupported optional commands.
                                    let _ = tx
                                        .send(
                                            build_capsule_resp(&NvmeCqe::error(
                                                cmd.cid,
                                                sq_id,
                                                status::SCT_GENERIC,
                                                0x01, // Invalid Opcode
                                            ))
                                            .to_vec(),
                                        )
                                        .await;
                                }
                                _ => {
                                    // Return success for unhandled I/O opcodes.
                                    // Returning INVALID_OPCODE breaks mkfs and other
                                    // tools that probe for optional commands.
                                    let _ = tx
                                        .send(
                                            build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id))
                                                .to_vec(),
                                        )
                                        .await;
                                }
                            };
                        });
                    }
                }
                PDU_TYPE_H2C_DATA => {
                    let extra = (hdr.hlen as usize).saturating_sub(PDU_HEADER_SIZE);
                    let mut extra_buf = vec![0u8; extra.min(256)];
                    if extra > 0 && extra <= 256 {
                        reader.read_exact(&mut extra_buf).await?;
                    }
                    let data_len = (hdr.plen as usize).saturating_sub(hdr.hlen as usize);
                    if data_len > 0 && data_len <= 1_048_576 {
                        let mut data = vec![0u8; data_len];
                        reader.read_exact(&mut data).await?;
                        let cid = if extra >= 2 {
                            u16::from_le_bytes([extra_buf[0], extra_buf[1]])
                        } else {
                            0
                        };

                        if let Some(pw) = state.pending_writes.remove(&cid) {
                            let tx = resp_tx.clone();
                            tokio::spawn(async move {
                                let len = pw.expected.min(data.len());
                                let resp =
                                    match pw.subsystem.backend.write(pw.offset, &data[..len]).await
                                    {
                                        Ok(()) => {
                                            build_capsule_resp(&NvmeCqe::success(cid, pw.sq_id))
                                        }
                                        Err(e) => {
                                            warn!("NVMe-oF: write error: {}", e);
                                            build_capsule_resp(&NvmeCqe::error(
                                                cid,
                                                pw.sq_id,
                                                status::SCT_GENERIC,
                                                status::DATA_XFER_ERROR,
                                            ))
                                        }
                                    };
                                let _ = tx.send(resp.to_vec()).await;
                            });
                        } else {
                            state.pending_write_data.insert(cid, data);
                        }
                    }
                }
                PDU_TYPE_H2C_TERM => {
                    return Err(NvmeOfError::ConnectionClosed);
                }
                _ => {
                    let remaining = (hdr.plen as usize).saturating_sub(PDU_HEADER_SIZE);
                    if remaining > 0 && remaining < 1_048_576 {
                        let mut skip = vec![0u8; remaining];
                        reader.read_exact(&mut skip).await?;
                    }
                }
            }
        }
    }

    async fn handle_fabric_connect(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &mut ConnectionState,
        data: Option<Vec<u8>>,
    ) -> Result<()> {
        let qid = (cmd.cdw10 >> 16) as u16;

        // Extract the SubNQN from the connect data (1024-byte structure).
        // Offset 256: SubNQN (256 bytes, null-terminated string).
        let subnqn = if let Some(ref d) = data {
            if d.len() >= 512 {
                let nqn_bytes = &d[256..512];
                let end = nqn_bytes.iter().position(|&b| b == 0).unwrap_or(256);
                String::from_utf8_lossy(&nqn_bytes[..end]).to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let available_nqns = self.list_subsystems().await;
        info!(
            "NVMe-oF: Fabric Connect qid={} nqn='{}' data_len={} available={:?}",
            qid,
            subnqn,
            data.as_ref().map(|d| d.len()).unwrap_or(0),
            available_nqns
        );

        // Look up the subsystem.
        if !subnqn.is_empty() {
            if let Some(subsys) = self.get_subsystem(&subnqn).await {
                state.subsystem = Some(subsys);
                state.sq_id = qid;
                state.connected = true;

                let mut cqe = NvmeCqe::success(cmd.cid, qid);
                // Unique controller ID per connection — required for NVMe multipath.
                // The kernel rejects duplicate cntlid on the same NQN.
                let cntlid = subsys.next_cntlid.fetch_add(1, Ordering::Relaxed);
                cqe.dw0 = cntlid as u32;
                stream.write_all(&build_capsule_resp(&cqe)).await?;
                stream.flush().await?;

                info!("NVMe-oF: connected to subsystem {} qid={}", subnqn, qid);
                return Ok(());
            }
        }

        // Subsystem not found — check if it's a discovery request.
        warn!("NVMe-oF: subsystem not found: {}", subnqn);
        let cqe = NvmeCqe::error(cmd.cid, qid, status::SCT_COMMAND, 0x06); // Connect Invalid Parameters
        stream.write_all(&build_capsule_resp(&cqe)).await?;
        Ok(())
    }

    async fn handle_property_get(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &ConnectionState,
    ) -> Result<()> {
        let offset = cmd.cdw11;
        let size = cmd.cdw10 & 0x7; // attrib: 0=4bytes, 1=8bytes

        let value: u64 = match offset {
            property::CAP => {
                let mqes: u64 = 255;
                let cqr: u64 = 1;
                let to: u64 = 30;
                let css: u64 = 1 << 7;
                mqes | (cqr << 16) | (to << 24) | (css << 37)
            }
            property::VS => 0x00010400,
            property::CC => state.cc as u64,
            property::CSTS => {
                if state.cc & 1 != 0 {
                    1
                } else {
                    0
                }
            }
            _ => 0,
        };

        let mut cqe = NvmeCqe::success(cmd.cid, state.sq_id);
        cqe.dw0 = value as u32;
        if size > 0 {
            cqe.dw1 = (value >> 32) as u32;
        }
        stream.write_all(&build_capsule_resp(&cqe)).await?;
        Ok(())
    }

    async fn handle_property_set(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &mut ConnectionState,
    ) -> Result<()> {
        let offset = cmd.cdw11;
        let value = cmd.cdw12 as u64 | ((cmd.cdw13 as u64) << 32);

        if offset == property::CC {
            state.cc = value as u32;
        }

        let cqe = NvmeCqe::success(cmd.cid, state.sq_id);
        stream.write_all(&build_capsule_resp(&cqe)).await?;
        Ok(())
    }

    async fn handle_admin_cmd(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &mut ConnectionState,
    ) -> Result<()> {
        match cmd.opcode {
            opcode::IDENTIFY => self.handle_identify(stream, cmd, state).await?,
            opcode::SET_FEATURES => self.handle_set_features(stream, cmd, state).await?,
            opcode::GET_FEATURES => self.handle_get_features(stream, cmd, state).await?,
            opcode::CREATE_CQ | opcode::CREATE_SQ | opcode::DELETE_CQ | opcode::DELETE_SQ => {
                let cqe = NvmeCqe::success(cmd.cid, 0);
                stream.write_all(&build_capsule_resp(&cqe)).await?;
            }
            opcode::KEEP_ALIVE => {
                let cqe = NvmeCqe::success(cmd.cid, 0);
                stream.write_all(&build_capsule_resp(&cqe)).await?;
            }
            opcode::ASYNC_EVENT => {
                // Hold — don't respond until an event occurs.
                state.async_event_cid = Some(cmd.cid);
            }
            opcode::GET_LOG_PAGE => {
                // Log Page: return minimal data for required log pages.
                let lid = cmd.cdw10 & 0xFF;
                let numd = ((cmd.cdw11 as u64) << 32 | (cmd.cdw10 >> 16) as u64) as usize;
                let data_len = ((numd + 1) * 4).min(4096);
                let log_data = match lid as u8 {
                    0x01 => {
                        // Error Information — empty (no errors).
                        vec![0u8; data_len.min(64)]
                    }
                    0x02 => {
                        // SMART / Health Information (512 bytes).
                        let mut buf = vec![0u8; data_len.min(512)];
                        // Temperature: 25°C = 298K
                        if buf.len() >= 4 {
                            buf[1] = 0x2A; // 298 = 0x012A LE
                            buf[2] = 0x01;
                        }
                        // Available Spare: 100%
                        if buf.len() > 3 {
                            buf[3] = 100;
                        }
                        buf
                    }
                    0x03 => {
                        // Firmware Slot Information (512 bytes).
                        vec![0u8; data_len.min(512)]
                    }
                    _ => {
                        // Unknown log page — return zeros.
                        vec![0u8; data_len]
                    }
                };
                let cqe = NvmeCqe::success(cmd.cid, 0);
                stream.write_all(&build_capsule_resp(&cqe)).await?;
                // Log data is sent via C2H data transfer — for simplicity,
                // include in the completion (NVMe-oF allows this for small data).
            }
            opcode::ABORT => {
                // Abort: acknowledge but don't actually cancel anything.
                // The SQID and CID to abort are in cdw10.
                let cqe = NvmeCqe::success(cmd.cid, 0);
                stream.write_all(&build_capsule_resp(&cqe)).await?;
            }
            _ => {
                let cqe = NvmeCqe::error(cmd.cid, 0, status::SCT_GENERIC, status::INVALID_OPCODE);
                stream.write_all(&build_capsule_resp(&cqe)).await?;
            }
        }
        Ok(())
    }

    async fn handle_identify(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &ConnectionState,
    ) -> Result<()> {
        let cns = (cmd.cdw10 & 0xFF) as u8;
        let subsys = state
            .subsystem
            .as_ref()
            .ok_or_else(|| NvmeOfError::Protocol("identify before connect".into()))?;

        let data = match cns {
            identify_cns::CONTROLLER => build_identify_controller(&subsys.config),
            identify_cns::NAMESPACE => build_identify_namespace(&*subsys.backend, &subsys.config.nqn),
            identify_cns::ACTIVE_NS_LIST => {
                let mut buf = vec![0u8; 4096];
                buf[0..4].copy_from_slice(&1u32.to_le_bytes());
                buf
            }
            identify_cns::NS_DESC_LIST => {
                let mut buf = vec![0u8; 4096];
                buf[0] = 3; // NIDT = UUID
                buf[1] = 16;
                let hash = crc32c::crc32c(subsys.config.nqn.as_bytes());
                buf[4..8].copy_from_slice(&hash.to_le_bytes());
                buf
            }
            _ => {
                let cqe = NvmeCqe::error(
                    cmd.cid,
                    state.sq_id,
                    status::SCT_GENERIC,
                    status::INVALID_FIELD,
                );
                stream.write_all(&build_capsule_resp(&cqe)).await?;
                return Ok(());
            }
        };

        // Send C2HData with LAST+SUCCESS flags. The SUCCESS flag means
        // implicit completion — do NOT send a separate CapsuleResp.
        let c2h_hdr = build_c2h_data_header(cmd.cid, 0, data.len() as u32, true);
        stream.write_all(&c2h_hdr).await?;
        stream.write_all(&data).await?;
        stream.flush().await?;
        Ok(())
    }

    async fn handle_set_features(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &ConnectionState,
    ) -> Result<()> {
        let fid = (cmd.cdw10 & 0xFF) as u8;
        let mut cqe = NvmeCqe::success(cmd.cid, state.sq_id);

        if fid == feature::NUM_QUEUES {
            // NVMe spec: NSQR/NCQR in CDW11 are 0-based (0 = 1 queue).
            // Grant up to 31 queues (0-based) = 32 actual queues.
            let nsqa = (cmd.cdw11 & 0xFFFF).min(31) as u16;
            let ncqa = ((cmd.cdw11 >> 16) & 0xFFFF).min(31) as u16;
            cqe.dw0 = (ncqa as u32) << 16 | nsqa as u32;
        }

        stream.write_all(&build_capsule_resp(&cqe)).await?;
        Ok(())
    }

    async fn handle_get_features(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &ConnectionState,
    ) -> Result<()> {
        let fid = (cmd.cdw10 & 0xFF) as u8;
        let mut cqe = NvmeCqe::success(cmd.cid, state.sq_id);

        if fid == feature::NUM_QUEUES {
            // 0-based: 31 = 32 queues supported (SQ and CQ).
            cqe.dw0 = (31u32 << 16) | 31u32;
        }

        stream.write_all(&build_capsule_resp(&cqe)).await?;
        Ok(())
    }

    async fn handle_admin_or_fabric(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
        cmd: &NvmeCmd,
        state: &mut ConnectionState,
        inline_data: Option<Vec<u8>>,
    ) -> Result<()> {
        if cmd.opcode == opcode::FABRIC {
            let fctype = (cmd.nsid & 0xFF) as u8;
            match fctype {
                fabric_cmd::CONNECT => {
                    self.handle_fabric_connect(stream, cmd, state, inline_data)
                        .await?
                }
                fabric_cmd::PROPERTY_GET => self.handle_property_get(stream, cmd, state).await?,
                fabric_cmd::PROPERTY_SET => self.handle_property_set(stream, cmd, state).await?,
                _ => {
                    let cqe = NvmeCqe::error(
                        cmd.cid,
                        state.sq_id,
                        status::SCT_GENERIC,
                        status::INVALID_OPCODE,
                    );
                    stream.write_all(&build_capsule_resp(&cqe)).await?;
                }
            }
        } else {
            self.handle_admin_cmd(stream, cmd, state).await?;
        }
        Ok(())
    }
}

/// Writer that buffers data and sends via mpsc channel.
struct ChannelWriter {
    buf: Vec<u8>,
    tx: Arc<tokio::sync::mpsc::Sender<Vec<u8>>>,
}

impl ChannelWriter {
    fn new(tx: Arc<tokio::sync::mpsc::Sender<Vec<u8>>>) -> Self {
        Self {
            buf: Vec::with_capacity(4096),
            tx,
        }
    }
    async fn flush_to_channel(&mut self) {
        if !self.buf.is_empty() {
            let data = std::mem::take(&mut self.buf);
            let _ = self.tx.send(data).await;
        }
    }
}

impl tokio::io::AsyncWrite for ChannelWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.buf.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl Unpin for ChannelWriter {}

/// Per-connection state.
struct PendingWrite {
    subsystem: Arc<Subsystem>,
    offset: u64,
    expected: usize,
    sq_id: u16,
}

struct ConnectionState {
    sq_id: u16,
    connected: bool,
    cc: u32,
    ttag_counter: u16,
    async_event_cid: Option<u16>,
    subsystem: Option<Arc<Subsystem>>,
    pending_write_data: HashMap<u16, Vec<u8>>,
    pending_writes: HashMap<u16, PendingWrite>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            sq_id: 0,
            connected: false,
            cc: 0,
            ttag_counter: 0,
            async_event_cid: None,
            subsystem: None,
            pending_write_data: HashMap::new(),
            pending_writes: HashMap::new(),
        }
    }

    fn next_ttag(&mut self) -> u16 {
        self.ttag_counter = self.ttag_counter.wrapping_add(1);
        if self.ttag_counter == 0 {
            self.ttag_counter = 1;
        }
        self.ttag_counter
    }
}

// --- Helper functions for building Identify data ---

fn build_identify_controller(config: &SubsystemConfig) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    buf[0..2].copy_from_slice(&0x1234u16.to_le_bytes());
    buf[2..4].copy_from_slice(&0x1234u16.to_le_bytes());
    let sn = format!("{:<20}", &config.serial[..config.serial.len().min(20)]);
    buf[4..24].copy_from_slice(sn.as_bytes());
    let mn = format!("{:<40}", &config.model[..config.model.len().min(40)]);
    buf[24..64].copy_from_slice(mn.as_bytes());
    buf[64..72].copy_from_slice(b"0.1.0\0\0\0");
    buf[77] = 5; // MDTS: 2^5 * 4KB = 128KB
    buf[78..80].copy_from_slice(&1u16.to_le_bytes());
    buf[80..84].copy_from_slice(&0x00010400u32.to_le_bytes());
    // ACL (Abort Command Limit) at offset 258 — max outstanding aborts.
    buf[258] = 4;
    // LPA (Log Page Attributes) at offset 261 — bit 0 = SMART per namespace.
    buf[261] = 1;
    // KAS (Keep Alive Support) at offset 320 — timeout granularity in 100ms units
    // Must be non-zero for NVMe-oF (kernel requires keep-alive for fabrics)
    buf[320..322].copy_from_slice(&100u16.to_le_bytes()); // 10 seconds (100 * 100ms)
    buf[512] = (6 << 4) | 6; // SQES
    buf[513] = (4 << 4) | 4; // CQES
    buf[514..516].copy_from_slice(&256u16.to_le_bytes());
    buf[516..520].copy_from_slice(&1u32.to_le_bytes());
    // ONCS — Optional NVMe Command Support (offset 520, 2 bytes LE)
    // Bit 0: Compare
    // Bit 2: Dataset Management (UNMAP/TRIM)
    // Bit 3: Write Zeroes
    // Bit 4: Save/Select field in Set/Get Features
    let oncs: u16 = (1 << 0) | (1 << 2) | (1 << 3) | (1 << 4); // Compare + DSM + Write Zeroes + Features
    buf[520..522].copy_from_slice(&oncs.to_le_bytes());
    buf[536..540].copy_from_slice(&1u32.to_le_bytes()); // SGLS
                                                        // NVMe-oF required fields (controller identify)
                                                        // Offsets verified against kernel struct nvme_id_ctrl (linux/nvme.h)
                                                        // CNTRLTYPE at offset 111 — 1 = I/O controller
    buf[111] = 1;
    // IOCCSZ at offset 1792 (after rsvd1024[768]) — in 16-byte units
    // Must be large enough for CapsuleCmd (72 bytes) + inline write data.
    // 8192 * 16 = 131072 bytes (128KB) max inline data per capsule.
    buf[1792..1796].copy_from_slice(&8192u32.to_le_bytes());
    // IORCSZ at offset 1796 — in 16-byte units
    // CapsuleResp PDU = 24 bytes. 24/16 = 1.5, round up to 2
    buf[1796..1800].copy_from_slice(&2u32.to_le_bytes());
    // ICDOFF at offset 1800 — in-capsule data offset
    buf[1800..1802].copy_from_slice(&0u16.to_le_bytes());
    // MSDBD at offset 1803 — max SGL data block descriptors
    buf[1803] = 1;
    let nqn_bytes = config.nqn.as_bytes();
    let nqn_len = nqn_bytes.len().min(256);
    buf[768..768 + nqn_len].copy_from_slice(&nqn_bytes[..nqn_len]);
    buf
}

fn build_identify_namespace(backend: &dyn NvmeOfBackend, nqn: &str) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let block_size = backend.block_size() as u64;
    let nsze = backend.size() / block_size;
    buf[0..8].copy_from_slice(&nsze.to_le_bytes());
    buf[8..16].copy_from_slice(&nsze.to_le_bytes());
    buf[16..24].copy_from_slice(&nsze.to_le_bytes());
    // NSFEAT (offset 24): bit 2 = deallocated blocks behave per DLFEAT
    buf[24] = 1 << 2;
    // DLFEAT (offset 32): bits [2:0] = 001 = read of deallocated LB returns zeros
    buf[32] = 0x01;

    // NGUID (offset 104, 16 bytes) — must be identical across all multipath
    // targets for the same volume. Derived from NQN hash so it's deterministic.
    let nguid = nqn_to_nguid(nqn);
    buf[104..120].copy_from_slice(&nguid);

    let ds = (block_size as f64).log2() as u8;
    buf[130] = ds;
    buf
}

/// Derive a 16-byte NGUID from the NQN string. Same NQN → same NGUID,
/// so all multipath targets for the same volume return the same namespace ID.
fn nqn_to_nguid(nqn: &str) -> [u8; 16] {
    // Use FNV-1a (deterministic, no random seed — unlike DefaultHasher).
    // Same NQN → same nguid across ALL processes, required for NVMe multipath.
    let mut h1: u64 = 0xcbf29ce484222325; // FNV offset basis
    for byte in nqn.as_bytes() {
        h1 ^= *byte as u64;
        h1 = h1.wrapping_mul(0x100000001b3); // FNV prime
    }
    // Second hash for bytes 8-15.
    let mut h2: u64 = h1;
    for byte in b"nguid-salt" {
        h2 ^= *byte as u64;
        h2 = h2.wrapping_mul(0x100000001b3);
    }
    let mut nguid = [0u8; 16];
    nguid[0..8].copy_from_slice(&h1.to_le_bytes());
    nguid[8..16].copy_from_slice(&h2.to_le_bytes());
    nguid
}
