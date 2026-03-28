//! NVMe-oF TCP PDU (Protocol Data Unit) types and framing.
//!
//! Implements the wire format for NVMe-oF over TCP as defined in the
//! NVM Express TCP Transport Specification.

use crate::error::{NvmeOfError, Result};

// PDU types
pub const PDU_TYPE_IC_REQ: u8 = 0x00;
pub const PDU_TYPE_IC_RESP: u8 = 0x01;
pub const PDU_TYPE_H2C_TERM: u8 = 0x02;
pub const PDU_TYPE_C2H_TERM: u8 = 0x03;
pub const PDU_TYPE_CAPSULE_CMD: u8 = 0x04;
pub const PDU_TYPE_CAPSULE_RESP: u8 = 0x05;
pub const PDU_TYPE_H2C_DATA: u8 = 0x06;
pub const PDU_TYPE_C2H_DATA: u8 = 0x07;
pub const PDU_TYPE_R2T: u8 = 0x09;

// PDU flags
pub const PDU_FLAG_HDGST: u8 = 0x01;
pub const PDU_FLAG_DDGST: u8 = 0x02;
pub const PDU_FLAG_LAST: u8 = 0x04;
pub const PDU_FLAG_SUCCESS: u8 = 0x08;

// ICReq/ICResp sizes
pub const IC_REQ_SIZE: usize = 128;
pub const IC_RESP_SIZE: usize = 128;

// Common PDU header size
pub const PDU_HEADER_SIZE: usize = 8;

// NVMe command size
pub const NVME_CMD_SIZE: usize = 64;

// NVMe completion size
pub const NVME_CQE_SIZE: usize = 16;

// CapsuleCmd header: common header (8) + command offset (8) = 16, then 64-byte NVMe command
pub const CAPSULE_CMD_HDR_SIZE: usize = 72; // 8 + 64

// CapsuleResp header: common header (8) + CQE (16) = 24
pub const CAPSULE_RESP_HDR_SIZE: usize = 24; // 8 + 16

// C2HData/H2CData header size
pub const DATA_PDU_HDR_SIZE: usize = 24;

// R2T header size
pub const R2T_HDR_SIZE: usize = 24;

/// Common PDU header (first 8 bytes of every PDU after ICReq/ICResp).
#[derive(Debug, Clone, Copy)]
pub struct PduHeader {
    pub pdu_type: u8,
    pub flags: u8,
    pub hlen: u8,  // Header length in 32-bit words
    pub pdo: u8,   // PDU data offset in 32-bit words
    pub plen: u32, // Total PDU length including header and data
}

impl PduHeader {
    pub fn decode(buf: &[u8; PDU_HEADER_SIZE]) -> Self {
        Self {
            pdu_type: buf[0],
            flags: buf[1],
            hlen: buf[2],
            pdo: buf[3],
            plen: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
        }
    }

    pub fn encode(&self, buf: &mut [u8; PDU_HEADER_SIZE]) {
        buf[0] = self.pdu_type;
        buf[1] = self.flags;
        buf[2] = self.hlen;
        buf[3] = self.pdo;
        buf[4..8].copy_from_slice(&self.plen.to_le_bytes());
    }
}

/// ICReq (Initialize Connection Request) — sent by initiator.
#[derive(Debug, Clone)]
pub struct IcReq {
    pub pdu_type: u8, // PDU_TYPE_IC_REQ
    pub hlen: u8,     // 128
    pub pfv: u16,     // PDU format version (0)
    pub maxr2t: u32,  // Max outstanding R2T requests
    pub hpda: u8,     // Host PDU data alignment (0 = no alignment)
    pub dgst: u8,     // Digest types enabled (bit 0=HDGST, bit 1=DDGST)
}

impl IcReq {
    pub fn decode(buf: &[u8; IC_REQ_SIZE]) -> Result<Self> {
        let pdu_type = buf[0];
        if pdu_type != PDU_TYPE_IC_REQ {
            return Err(NvmeOfError::Protocol(format!(
                "expected ICReq type 0x{:02X}, got 0x{:02X}",
                PDU_TYPE_IC_REQ, pdu_type
            )));
        }
        // Layout: bytes 0-7 = standard PDU header, then ICReq-specific fields:
        //   bytes 8-9:   pfv (PDU format version)
        //   byte  10:    hpda (host PDU data alignment)
        //   byte  11:    dgst (digest types: bit 0=HDGST, bit 1=DDGST)
        //   bytes 12-15: maxr2t
        Ok(Self {
            pdu_type,
            hlen: buf[2],
            pfv: u16::from_le_bytes([buf[8], buf[9]]),
            hpda: buf[10],
            dgst: buf[11],
            maxr2t: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
        })
    }
}

/// ICResp (Initialize Connection Response) — sent by target.
#[derive(Debug, Clone)]
pub struct IcResp {
    pub pfv: u16,
    pub cpda: u8,     // Controller PDU data alignment
    pub dgst: u8,     // Digest types enabled
    pub maxdata: u32, // Maximum data capsule size
}

impl IcResp {
    pub fn encode(&self) -> [u8; IC_RESP_SIZE] {
        let mut buf = [0u8; IC_RESP_SIZE];
        // Standard PDU header (8 bytes)
        buf[0] = PDU_TYPE_IC_RESP;
        buf[1] = 0; // flags
        buf[2] = IC_RESP_SIZE as u8; // hlen
        buf[3] = 0; // pdo
        buf[4..8].copy_from_slice(&(IC_RESP_SIZE as u32).to_le_bytes()); // plen
                                                                         // ICResp-specific fields
        buf[8..10].copy_from_slice(&self.pfv.to_le_bytes());
        buf[10] = self.cpda;
        buf[11] = self.dgst;
        buf[12..16].copy_from_slice(&self.maxdata.to_le_bytes());
        buf
    }
}

/// 64-byte NVMe command (from CapsuleCmd).
#[derive(Debug, Clone, Copy)]
pub struct NvmeCmd {
    pub opcode: u8,
    pub flags: u8,
    pub cid: u16,  // Command ID
    pub nsid: u32, // Namespace ID
    pub cdw2: u32,
    pub cdw3: u32,
    pub metadata: u64,
    pub dptr: [u64; 2], // Data pointer (PRP or SGL)
    pub cdw10: u32,
    pub cdw11: u32,
    pub cdw12: u32,
    pub cdw13: u32,
    pub cdw14: u32,
    pub cdw15: u32,
}

impl NvmeCmd {
    pub fn decode(buf: &[u8; NVME_CMD_SIZE]) -> Self {
        Self {
            opcode: buf[0],
            flags: buf[1],
            cid: u16::from_le_bytes([buf[2], buf[3]]),
            nsid: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            cdw2: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            cdw3: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
            metadata: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            dptr: [
                u64::from_le_bytes(buf[24..32].try_into().unwrap()),
                u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            ],
            cdw10: u32::from_le_bytes([buf[40], buf[41], buf[42], buf[43]]),
            cdw11: u32::from_le_bytes([buf[44], buf[45], buf[46], buf[47]]),
            cdw12: u32::from_le_bytes([buf[48], buf[49], buf[50], buf[51]]),
            cdw13: u32::from_le_bytes([buf[52], buf[53], buf[54], buf[55]]),
            cdw14: u32::from_le_bytes([buf[56], buf[57], buf[58], buf[59]]),
            cdw15: u32::from_le_bytes([buf[60], buf[61], buf[62], buf[63]]),
        }
    }

    /// Get the SGL descriptor from dptr (used in NVMe-oF TCP).
    pub fn sgl_desc(&self) -> SglDesc {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.dptr[0].to_le_bytes());
        buf[8..16].copy_from_slice(&self.dptr[1].to_le_bytes());
        SglDesc::decode(&buf)
    }
}

/// SGL (Scatter Gather List) descriptor — used in NVMe-oF for data transfer.
#[derive(Debug, Clone, Copy)]
pub struct SglDesc {
    pub address: u64,
    pub length: u32,
    pub sgl_type: u8, // Type (bits 7:4) and subtype (bits 3:0)
}

impl SglDesc {
    pub fn decode(buf: &[u8; 16]) -> Self {
        Self {
            address: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            length: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            sgl_type: buf[15],
        }
    }

    /// Check if this is an inline data SGL (type 0x0, subtype 0x1).
    pub fn is_data_block(&self) -> bool {
        self.sgl_type == 0x00
    }
}

/// 16-byte NVMe Completion Queue Entry.
#[derive(Debug, Clone, Copy)]
pub struct NvmeCqe {
    pub dw0: u32,     // Command-specific result
    pub dw1: u32,     // Reserved
    pub sq_head: u16, // Submission queue head pointer
    pub sq_id: u16,   // Submission queue ID
    pub cid: u16,     // Command ID
    pub status: u16,  // Status field (bit 0 = phase, bits 1-15 = status code)
}

impl NvmeCqe {
    pub fn success(cid: u16, sq_id: u16) -> Self {
        Self {
            dw0: 0,
            dw1: 0,
            sq_head: 0,
            sq_id,
            cid,
            status: 0, // Success, phase=0
        }
    }

    pub fn error(cid: u16, sq_id: u16, sct: u8, sc: u8) -> Self {
        // Status field: bits 9:1 = Status Code Type (SCT), bits 8:1 = Status Code (SC)
        let status = ((sct as u16 & 0x7) << 9) | ((sc as u16) << 1);
        Self {
            dw0: 0,
            dw1: 0,
            sq_head: 0,
            sq_id,
            cid,
            status,
        }
    }

    pub fn encode(&self, buf: &mut [u8; NVME_CQE_SIZE]) {
        buf[0..4].copy_from_slice(&self.dw0.to_le_bytes());
        buf[4..8].copy_from_slice(&self.dw1.to_le_bytes());
        buf[8..10].copy_from_slice(&self.sq_head.to_le_bytes());
        buf[10..12].copy_from_slice(&self.sq_id.to_le_bytes());
        buf[12..14].copy_from_slice(&self.cid.to_le_bytes());
        buf[14..16].copy_from_slice(&self.status.to_le_bytes());
    }
}

/// Build a CapsuleResp PDU (header + CQE).
pub fn build_capsule_resp(cqe: &NvmeCqe) -> [u8; CAPSULE_RESP_HDR_SIZE] {
    let mut buf = [0u8; CAPSULE_RESP_HDR_SIZE];
    let hdr = PduHeader {
        pdu_type: PDU_TYPE_CAPSULE_RESP,
        flags: 0,
        hlen: CAPSULE_RESP_HDR_SIZE as u8,
        pdo: 0,
        plen: CAPSULE_RESP_HDR_SIZE as u32,
    };
    let mut hdr_buf = [0u8; PDU_HEADER_SIZE];
    hdr.encode(&mut hdr_buf);
    buf[0..PDU_HEADER_SIZE].copy_from_slice(&hdr_buf);

    let mut cqe_buf = [0u8; NVME_CQE_SIZE];
    cqe.encode(&mut cqe_buf);
    buf[PDU_HEADER_SIZE..PDU_HEADER_SIZE + NVME_CQE_SIZE].copy_from_slice(&cqe_buf);
    buf
}

/// Build a C2HData PDU header (for read responses).
pub fn build_c2h_data_header(
    cid: u16,
    data_offset: u32,
    data_len: u32,
    last: bool,
) -> [u8; DATA_PDU_HDR_SIZE] {
    let mut buf = [0u8; DATA_PDU_HDR_SIZE];
    let mut flags = 0u8;
    if last {
        flags |= PDU_FLAG_LAST | PDU_FLAG_SUCCESS;
    }
    let hdr = PduHeader {
        pdu_type: PDU_TYPE_C2H_DATA,
        flags,
        hlen: DATA_PDU_HDR_SIZE as u8,
        pdo: DATA_PDU_HDR_SIZE as u8,
        plen: DATA_PDU_HDR_SIZE as u32 + data_len,
    };
    let mut hdr_buf = [0u8; PDU_HEADER_SIZE];
    hdr.encode(&mut hdr_buf);
    buf[0..PDU_HEADER_SIZE].copy_from_slice(&hdr_buf);

    // C2HData-specific fields after common header
    buf[8..10].copy_from_slice(&cid.to_le_bytes());
    // ttag (transfer tag) at bytes 10-11 — 0 for unsolicited
    buf[12..16].copy_from_slice(&data_offset.to_le_bytes());
    buf[16..20].copy_from_slice(&data_len.to_le_bytes());
    buf
}

/// Build an R2T PDU (ready to transfer — tells host to send write data).
pub fn build_r2t(cid: u16, ttag: u16, data_offset: u32, data_len: u32) -> [u8; R2T_HDR_SIZE] {
    let mut buf = [0u8; R2T_HDR_SIZE];
    let hdr = PduHeader {
        pdu_type: PDU_TYPE_R2T,
        flags: 0,
        hlen: R2T_HDR_SIZE as u8,
        pdo: 0,
        plen: R2T_HDR_SIZE as u32,
    };
    let mut hdr_buf = [0u8; PDU_HEADER_SIZE];
    hdr.encode(&mut hdr_buf);
    buf[0..PDU_HEADER_SIZE].copy_from_slice(&hdr_buf);

    buf[8..10].copy_from_slice(&cid.to_le_bytes());
    buf[10..12].copy_from_slice(&ttag.to_le_bytes());
    buf[12..16].copy_from_slice(&data_offset.to_le_bytes());
    buf[16..20].copy_from_slice(&data_len.to_le_bytes());
    buf
}

// NVMe opcodes
pub mod opcode {
    // Admin commands
    pub const DELETE_SQ: u8 = 0x00;
    pub const CREATE_SQ: u8 = 0x01;
    pub const DELETE_CQ: u8 = 0x04;
    pub const CREATE_CQ: u8 = 0x05;
    pub const IDENTIFY: u8 = 0x06;
    pub const GET_LOG_PAGE: u8 = 0x02;
    pub const ABORT: u8 = 0x08;
    pub const SET_FEATURES: u8 = 0x09;
    pub const GET_FEATURES: u8 = 0x0A;
    pub const ASYNC_EVENT: u8 = 0x0C;
    pub const KEEP_ALIVE: u8 = 0x18;

    // I/O commands
    pub const FLUSH: u8 = 0x00;
    pub const WRITE: u8 = 0x01;
    pub const READ: u8 = 0x02;
    pub const WRITE_UNCORRECTABLE: u8 = 0x04;
    pub const COMPARE: u8 = 0x05;
    pub const WRITE_ZEROES: u8 = 0x08;
    pub const DATASET_MGMT: u8 = 0x09; // Trim/Deallocate
    pub const COPY: u8 = 0x19;
    pub const RESERVATION_REGISTER: u8 = 0x0D;
    pub const RESERVATION_REPORT: u8 = 0x0E;
    pub const RESERVATION_ACQUIRE: u8 = 0x11;
    pub const RESERVATION_RELEASE: u8 = 0x15;

    // Fabric commands
    pub const FABRIC: u8 = 0x7F;
}

/// NVMe Dataset Management range descriptor (16 bytes, little-endian).
pub struct DsmRange {
    pub context_attrs: u32,
    pub length_blocks: u32,
    pub starting_lba: u64,
}

impl DsmRange {
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 16 {
            return None;
        }
        Some(Self {
            context_attrs: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
            length_blocks: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            starting_lba: u64::from_le_bytes([
                buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            ]),
        })
    }

    pub fn parse_ranges(data: &[u8], count: usize) -> Vec<Self> {
        (0..count)
            .filter_map(|i| {
                let offset = i * 16;
                if offset + 16 <= data.len() {
                    Self::from_bytes(&data[offset..offset + 16])
                } else {
                    None
                }
            })
            .collect()
    }
}

// Fabric command types (in CDW10 of fabric command)
pub mod fabric_cmd {
    pub const PROPERTY_SET: u8 = 0x00;
    pub const CONNECT: u8 = 0x01;
    pub const PROPERTY_GET: u8 = 0x04;
    pub const AUTH_SEND: u8 = 0x05;
    pub const AUTH_RECV: u8 = 0x06;
    pub const DISCONNECT: u8 = 0x08;
}

// NVMe status codes
pub mod status {
    pub const SUCCESS: u8 = 0x00;
    pub const INVALID_OPCODE: u8 = 0x01;
    pub const INVALID_FIELD: u8 = 0x02;
    pub const DATA_XFER_ERROR: u8 = 0x04;
    pub const INTERNAL_ERROR: u8 = 0x06;
    pub const INVALID_NS: u8 = 0x0B;

    // Status Code Types
    pub const SCT_GENERIC: u8 = 0x00;
    pub const SCT_COMMAND: u8 = 0x01;
}

// Identify CNS values
pub mod identify_cns {
    pub const NAMESPACE: u8 = 0x00;
    pub const CONTROLLER: u8 = 0x01;
    pub const ACTIVE_NS_LIST: u8 = 0x02;
    pub const NS_DESC_LIST: u8 = 0x03;
}

// Feature IDs
pub mod feature {
    pub const ARBITRATION: u8 = 0x01;
    pub const POWER_MGMT: u8 = 0x02;
    pub const TEMP_THRESHOLD: u8 = 0x04;
    pub const ERROR_RECOVERY: u8 = 0x05;
    pub const NUM_QUEUES: u8 = 0x07;
    pub const WRITE_ATOMICITY: u8 = 0x0A;
    pub const ASYNC_EVENT: u8 = 0x0B;
    pub const KEEP_ALIVE_TIMER: u8 = 0x0F;
}

// NVMe-oF controller properties (registers)
pub mod property {
    pub const CAP: u32 = 0x00; // Controller Capabilities (8 bytes)
    pub const VS: u32 = 0x08; // Version (4 bytes)
    pub const CC: u32 = 0x14; // Controller Configuration (4 bytes)
    pub const CSTS: u32 = 0x1C; // Controller Status (4 bytes)
}
