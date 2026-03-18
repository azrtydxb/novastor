//! NBD wire protocol constants and types.
//!
//! Implements the "newstyle fixed" NBD handshake and transmission protocol
//! as documented in https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md

// --- Handshake phase ---

/// Server sends this magic during initial handshake.
pub const NBD_MAGIC: u64 = 0x4e42444d41474943; // "NBDMAGIC"
/// Option magic (newstyle).
pub const NBD_OPTS_MAGIC: u64 = 0x49484156454F5054; // "IHAVEOPT"
/// Clat magic in option replies.
pub const NBD_REP_MAGIC: u64 = 0x3e889045565a9;

// Handshake flags (server → client).
pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// Client flags.
pub const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1 << 0;
pub const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

// Transmission flags (per-export).
pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const NBD_FLAG_READ_ONLY: u16 = 1 << 1;
pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
pub const NBD_FLAG_SEND_FUA: u16 = 1 << 3;
pub const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
pub const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;

// Option types (client → server during handshake).
pub const NBD_OPT_EXPORT_NAME: u32 = 1;
pub const NBD_OPT_ABORT: u32 = 2;
pub const NBD_OPT_LIST: u32 = 3;
pub const NBD_OPT_GO: u32 = 7;

// Option reply types.
pub const NBD_REP_ACK: u32 = 1;
pub const NBD_REP_SERVER: u32 = 2;
pub const NBD_REP_INFO: u32 = 3;
pub const NBD_REP_ERR_UNSUP: u32 = (1 << 31) | 1;
pub const NBD_REP_ERR_INVALID: u32 = (1 << 31) | 3;

// Info types for NBD_OPT_GO / NBD_REP_INFO.
pub const NBD_INFO_EXPORT: u16 = 0;

// --- Transmission phase ---

/// Request magic.
pub const NBD_REQUEST_MAGIC: u32 = 0x25609513;
/// Reply magic.
pub const NBD_REPLY_MAGIC: u32 = 0x67446698;

/// NBD command types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum NbdCommand {
    Read = 0,
    Write = 1,
    Disc = 2,  // Disconnect
    Flush = 3,
    Trim = 4,
    WriteZeroes = 6,
}

impl NbdCommand {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(Self::Read),
            1 => Some(Self::Write),
            2 => Some(Self::Disc),
            3 => Some(Self::Flush),
            4 => Some(Self::Trim),
            6 => Some(Self::WriteZeroes),
            _ => None,
        }
    }
}

/// Parsed NBD request (transmission phase).
#[derive(Debug)]
pub struct NbdRequest {
    pub flags: u16,
    pub command: NbdCommand,
    pub handle: u64,
    pub offset: u64,
    pub length: u32,
}

// Error codes for NBD replies.
pub const NBD_OK: u32 = 0;
pub const NBD_EIO: u32 = 5;
pub const NBD_ENOMEM: u32 = 12;
pub const NBD_EINVAL: u32 = 22;
pub const NBD_ENOSPC: u32 = 28;
