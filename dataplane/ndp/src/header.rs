use crate::error::{NdpError, Result};

pub const NDP_MAGIC: u32 = 0x4E565354; // "NVST"
pub const NDP_HEADER_SIZE: usize = 64;

/// NDP header flag: write is a migration — target checks generation before accepting.
pub const FLAG_MIGRATION: u8 = 0x01;

/// NDP operation codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NdpOp {
    Write = 0x01,
    Read = 0x02,
    WriteResp = 0x03,
    ReadResp = 0x04,
    WriteZeroes = 0x05,
    Unmap = 0x06,
    Replicate = 0x07,
    ReplicateResp = 0x08,
    EcShard = 0x09,
    Ping = 0x0A,
    Pong = 0x0B,
    ChunkMapSync = 0x0C,
    ChunkMapSyncResp = 0x0D,
    RegisterVolume = 0x0E,
    RegisterVolumeResp = 0x0F,
}

impl NdpOp {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0x01 => Ok(Self::Write),
            0x02 => Ok(Self::Read),
            0x03 => Ok(Self::WriteResp),
            0x04 => Ok(Self::ReadResp),
            0x05 => Ok(Self::WriteZeroes),
            0x06 => Ok(Self::Unmap),
            0x07 => Ok(Self::Replicate),
            0x08 => Ok(Self::ReplicateResp),
            0x09 => Ok(Self::EcShard),
            0x0A => Ok(Self::Ping),
            0x0B => Ok(Self::Pong),
            0x0C => Ok(Self::ChunkMapSync),
            0x0D => Ok(Self::ChunkMapSyncResp),
            0x0E => Ok(Self::RegisterVolume),
            0x0F => Ok(Self::RegisterVolumeResp),
            other => Err(NdpError::UnknownOp(other)),
        }
    }

    pub fn has_request_data(self) -> bool {
        matches!(
            self,
            Self::Write
                | Self::Replicate
                | Self::EcShard
                | Self::ChunkMapSync
                | Self::RegisterVolume
        )
    }

    pub fn has_response_data(self) -> bool {
        matches!(self, Self::ReadResp)
    }
}

/// Fixed 64-byte NDP header. All fields are little-endian on the wire.
#[derive(Debug, Clone, Copy)]
pub struct NdpHeader {
    pub op: NdpOp,
    pub flags: u8,
    pub status: u16,
    pub request_id: u64,
    pub volume_hash: u64,
    pub offset: u64,
    pub data_length: u32,
    pub chunk_idx: u16,
    pub sb_idx: u8,
    pub header_crc: u32,
    pub data_crc: u32,
}

impl NdpHeader {
    /// Encode the header into a 64-byte buffer.
    pub fn encode(&self, buf: &mut [u8; NDP_HEADER_SIZE]) {
        buf[0..4].copy_from_slice(&NDP_MAGIC.to_le_bytes());
        buf[4] = self.op as u8;
        buf[5] = self.flags;
        buf[6..8].copy_from_slice(&self.status.to_le_bytes());
        buf[8..16].copy_from_slice(&self.request_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.volume_hash.to_le_bytes());
        buf[24..32].copy_from_slice(&self.offset.to_le_bytes());
        buf[32..36].copy_from_slice(&self.data_length.to_le_bytes());
        buf[36..38].copy_from_slice(&self.chunk_idx.to_le_bytes());
        buf[38] = self.sb_idx;
        buf[39] = 0; // pad
        buf[40..48].fill(0); // reserved
        buf[52..56].copy_from_slice(&self.data_crc.to_le_bytes());
        buf[56..64].fill(0); // reserved
                             // Compute header CRC over bytes 0..48 (before CRC fields).
        let crc = crc32c::crc32c(&buf[0..48]);
        buf[48..52].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decode a 64-byte buffer into an NdpHeader.
    pub fn decode(buf: &[u8; NDP_HEADER_SIZE]) -> Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != NDP_MAGIC {
            return Err(NdpError::InvalidMagic(magic));
        }

        // Verify header CRC.
        let expected_crc = u32::from_le_bytes(buf[48..52].try_into().unwrap());
        let actual_crc = crc32c::crc32c(&buf[0..48]);
        if expected_crc != actual_crc {
            return Err(NdpError::HeaderCrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        Ok(Self {
            op: NdpOp::from_u8(buf[4])?,
            flags: buf[5],
            status: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            request_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            volume_hash: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            offset: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            data_length: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
            chunk_idx: u16::from_le_bytes(buf[36..38].try_into().unwrap()),
            sb_idx: buf[38],
            header_crc: expected_crc,
            data_crc: u32::from_le_bytes(buf[52..56].try_into().unwrap()),
        })
    }

    /// Create a new request header.
    pub fn request(op: NdpOp, request_id: u64, volume_hash: u64, offset: u64, length: u32) -> Self {
        Self {
            op,
            flags: 0,
            status: 0,
            request_id,
            volume_hash,
            offset,
            data_length: length,
            chunk_idx: 0,
            sb_idx: 0,
            header_crc: 0,
            data_crc: 0,
        }
    }

    /// Create a response header for a given request.
    pub fn response(req: &NdpHeader, op: NdpOp, status: u16, data_length: u32) -> Self {
        Self {
            op,
            flags: 0,
            status,
            request_id: req.request_id,
            volume_hash: req.volume_hash,
            offset: req.offset,
            data_length,
            chunk_idx: req.chunk_idx,
            sb_idx: req.sb_idx,
            header_crc: 0,
            data_crc: 0,
        }
    }
}

/// Compute a volume hash from a volume UUID string.
pub fn volume_hash(uuid: &str) -> u64 {
    let crc1 = crc32c::crc32c(uuid.as_bytes()) as u64;
    let crc2 = crc32c::crc32c(&uuid.as_bytes().iter().rev().copied().collect::<Vec<u8>>()) as u64;
    (crc1 << 32) | crc2
}
