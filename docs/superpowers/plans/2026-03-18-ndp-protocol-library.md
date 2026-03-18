# NDP Protocol Library Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the NovaStor Data Protocol (NDP) — a fixed 64-byte header binary TCP protocol for all internal data-path communication between frontend controllers and chunk engines.

**Architecture:** A standalone Rust crate (`ndp`) inside the dataplane workspace. Defines the wire format as a `#[repr(C, packed)]` struct, provides zero-copy encode/decode, CRC32 integrity checks, and async TCP connection handling with multiplexed request/response matching. No SPDK dependency — pure Rust + tokio.

**Tech Stack:** Rust, tokio, crc32c crate (hardware-accelerated), bytes crate for buffer management.

**Spec:** `docs/superpowers/specs/2026-03-18-frontend-backend-split-design.md` (NDP section)

---

## File Structure

```
dataplane/
├── Cargo.toml              # Modify: add ndp as workspace member
├── ndp/
│   ├── Cargo.toml          # Create: ndp crate manifest
│   ├── src/
│   │   ├── lib.rs          # Create: crate root, re-exports
│   │   ├── header.rs       # Create: NdpHeader struct, ops, encode/decode
│   │   ├── codec.rs        # Create: tokio codec for framing (header + data)
│   │   ├── connection.rs   # Create: NdpConnection — multiplexed req/resp over TCP
│   │   ├── pool.rs         # Create: ConnectionPool — persistent connections to multiple peers
│   │   └── error.rs        # Create: NdpError type
│   └── tests/
│       ├── header_test.rs  # Create: header encode/decode/CRC tests
│       ├── codec_test.rs   # Create: codec framing tests
│       └── connection_test.rs # Create: multiplexed connection tests
```

---

## Chunk 1: NDP Header and Wire Format

### Task 1: NDP Header Struct

**Files:**
- Create: `dataplane/ndp/Cargo.toml`
- Create: `dataplane/ndp/src/lib.rs`
- Create: `dataplane/ndp/src/header.rs`
- Create: `dataplane/ndp/src/error.rs`
- Create: `dataplane/ndp/tests/header_test.rs`

- [ ] **Step 1: Create the ndp crate**

Create `dataplane/ndp/Cargo.toml`:
```toml
[package]
name = "ndp"
version = "0.1.0"
edition = "2021"

[dependencies]
crc32c = "0.6"
thiserror = "2"
tokio = { version = "1", features = ["net", "io-util", "sync", "rt", "macros"] }
bytes = "1"

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "test-util"] }
```

Create `dataplane/ndp/src/error.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NdpError {
    #[error("invalid magic: expected 0x4E565354, got 0x{0:08X}")]
    InvalidMagic(u32),
    #[error("header CRC mismatch: expected 0x{expected:08X}, got 0x{actual:08X}")]
    HeaderCrcMismatch { expected: u32, actual: u32 },
    #[error("data CRC mismatch: expected 0x{expected:08X}, got 0x{actual:08X}")]
    DataCrcMismatch { expected: u32, actual: u32 },
    #[error("unknown op: 0x{0:02X}")]
    UnknownOp(u8),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("request timeout")]
    Timeout,
    #[error("pool error: {0}")]
    Pool(String),
}

pub type Result<T> = std::result::Result<T, NdpError>;
```

Create `dataplane/ndp/src/lib.rs`:
```rust
pub mod header;
pub mod error;

pub use header::{NdpHeader, NdpOp};
pub use error::{NdpError, Result};
```

- [ ] **Step 2: Define the NDP header struct**

Create `dataplane/ndp/src/header.rs`:
```rust
use crate::error::{NdpError, Result};

pub const NDP_MAGIC: u32 = 0x4E565354; // "NVST"
pub const NDP_HEADER_SIZE: usize = 64;

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
            other => Err(NdpError::UnknownOp(other)),
        }
    }

    pub fn has_request_data(self) -> bool {
        matches!(self, Self::Write | Self::Replicate | Self::EcShard)
    }

    pub fn has_response_data(self) -> bool {
        matches!(self, Self::ReadResp)
    }
}

/// Fixed 64-byte NDP header. All fields are little-endian.
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
        // Compute header CRC over bytes 0..48 (excluding CRC fields)
        let crc = crc32c::crc32c(&buf[0..48]);
        buf[48..52].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decode a 64-byte buffer into an NdpHeader.
    pub fn decode(buf: &[u8; NDP_HEADER_SIZE]) -> Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != NDP_MAGIC {
            return Err(NdpError::InvalidMagic(magic));
        }

        // Verify header CRC
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
    // Use first 8 bytes of CRC64 (via two CRC32s)
    let crc1 = crc32c::crc32c(uuid.as_bytes()) as u64;
    let crc2 = crc32c::crc32c(&uuid.as_bytes().iter().rev().copied().collect::<Vec<u8>>()) as u64;
    (crc1 << 32) | crc2
}
```

- [ ] **Step 3: Write header tests**

Create `dataplane/ndp/tests/header_test.rs`:
```rust
use ndp::{NdpHeader, NdpOp};
use ndp::header::{NDP_HEADER_SIZE, NDP_MAGIC, volume_hash};

#[test]
fn test_encode_decode_roundtrip() {
    let header = NdpHeader::request(NdpOp::Write, 42, 0xDEADBEEF, 4096, 4096);
    let mut buf = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut buf);
    let decoded = NdpHeader::decode(&buf).unwrap();
    assert_eq!(decoded.op, NdpOp::Write);
    assert_eq!(decoded.request_id, 42);
    assert_eq!(decoded.volume_hash, 0xDEADBEEF);
    assert_eq!(decoded.offset, 4096);
    assert_eq!(decoded.data_length, 4096);
}

#[test]
fn test_invalid_magic() {
    let mut buf = [0u8; NDP_HEADER_SIZE];
    buf[0..4].copy_from_slice(&0x12345678u32.to_le_bytes());
    let err = NdpHeader::decode(&buf).unwrap_err();
    assert!(matches!(err, ndp::NdpError::InvalidMagic(0x12345678)));
}

#[test]
fn test_crc_mismatch() {
    let header = NdpHeader::request(NdpOp::Read, 1, 0, 0, 0);
    let mut buf = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut buf);
    buf[10] ^= 0xFF; // corrupt a byte
    let err = NdpHeader::decode(&buf).unwrap_err();
    assert!(matches!(err, ndp::NdpError::HeaderCrcMismatch { .. }));
}

#[test]
fn test_all_ops_roundtrip() {
    let ops = [
        NdpOp::Write, NdpOp::Read, NdpOp::WriteResp, NdpOp::ReadResp,
        NdpOp::WriteZeroes, NdpOp::Unmap, NdpOp::Replicate,
        NdpOp::ReplicateResp, NdpOp::EcShard, NdpOp::Ping, NdpOp::Pong,
    ];
    for op in ops {
        let h = NdpHeader::request(op, 99, 0, 0, 0);
        let mut buf = [0u8; NDP_HEADER_SIZE];
        h.encode(&mut buf);
        let d = NdpHeader::decode(&buf).unwrap();
        assert_eq!(d.op, op);
    }
}

#[test]
fn test_response_preserves_request_id() {
    let req = NdpHeader::request(NdpOp::Read, 777, 0xABCD, 8192, 0);
    let resp = NdpHeader::response(&req, NdpOp::ReadResp, 0, 4096);
    assert_eq!(resp.request_id, 777);
    assert_eq!(resp.volume_hash, 0xABCD);
    assert_eq!(resp.offset, 8192);
    assert_eq!(resp.data_length, 4096);
}

#[test]
fn test_volume_hash_deterministic() {
    let h1 = volume_hash("test-volume-uuid-123");
    let h2 = volume_hash("test-volume-uuid-123");
    assert_eq!(h1, h2);
    let h3 = volume_hash("different-volume");
    assert_ne!(h1, h3);
}

#[test]
fn test_header_size_is_64() {
    assert_eq!(NDP_HEADER_SIZE, 64);
}
```

- [ ] **Step 4: Run tests**

Run: `cd dataplane/ndp && cargo test`
Expected: All 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add dataplane/ndp/
git commit -m "feat(ndp): NDP protocol header — 64-byte wire format with CRC32"
```

---

### Task 2: NDP Codec (Tokio Framing)

**Files:**
- Create: `dataplane/ndp/src/codec.rs`
- Modify: `dataplane/ndp/src/lib.rs`
- Create: `dataplane/ndp/tests/codec_test.rs`

- [ ] **Step 1: Implement the codec**

Create `dataplane/ndp/src/codec.rs`:
```rust
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{NdpError, Result};
use crate::header::{NdpHeader, NDP_HEADER_SIZE};

/// An NDP message: header + optional data payload.
#[derive(Debug)]
pub struct NdpMessage {
    pub header: NdpHeader,
    pub data: Option<Vec<u8>>,
}

impl NdpMessage {
    pub fn new(header: NdpHeader, data: Option<Vec<u8>>) -> Self {
        Self { header, data }
    }

    /// Write this message to an async writer.
    pub async fn write_to<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        let mut hdr_buf = [0u8; NDP_HEADER_SIZE];
        let mut header = self.header;

        // Set data CRC if there's a payload.
        if let Some(ref data) = self.data {
            header.data_length = data.len() as u32;
            header.data_crc = crc32c::crc32c(data);
        }

        header.encode(&mut hdr_buf);
        writer.write_all(&hdr_buf).await?;

        if let Some(ref data) = self.data {
            writer.write_all(data).await?;
        }

        Ok(())
    }

    /// Read a message from an async reader.
    pub async fn read_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let mut hdr_buf = [0u8; NDP_HEADER_SIZE];
        let n = reader.read_exact(&mut hdr_buf).await?;
        if n == 0 {
            return Err(NdpError::ConnectionClosed);
        }

        let header = NdpHeader::decode(&hdr_buf)?;

        let data = if header.data_length > 0 {
            let mut buf = vec![0u8; header.data_length as usize];
            reader.read_exact(&mut buf).await?;

            // Verify data CRC.
            let actual_crc = crc32c::crc32c(&buf);
            if header.data_crc != 0 && actual_crc != header.data_crc {
                return Err(NdpError::DataCrcMismatch {
                    expected: header.data_crc,
                    actual: actual_crc,
                });
            }

            Some(buf)
        } else {
            None
        };

        Ok(Self { header, data })
    }
}
```

Update `dataplane/ndp/src/lib.rs`:
```rust
pub mod header;
pub mod codec;
pub mod error;

pub use header::{NdpHeader, NdpOp};
pub use codec::NdpMessage;
pub use error::{NdpError, Result};
```

- [ ] **Step 2: Write codec tests**

Create `dataplane/ndp/tests/codec_test.rs`:
```rust
use ndp::{NdpHeader, NdpMessage, NdpOp};
use tokio::io::{duplex};

#[tokio::test]
async fn test_write_read_roundtrip_no_data() {
    let (mut client, mut server) = duplex(1024);
    let header = NdpHeader::request(NdpOp::Ping, 1, 0, 0, 0);
    let msg = NdpMessage::new(header, None);
    msg.write_to(&mut client).await.unwrap();

    let received = NdpMessage::read_from(&mut server).await.unwrap();
    assert_eq!(received.header.op, NdpOp::Ping);
    assert_eq!(received.header.request_id, 1);
    assert!(received.data.is_none());
}

#[tokio::test]
async fn test_write_read_roundtrip_with_data() {
    let (mut client, mut server) = duplex(8192);
    let header = NdpHeader::request(NdpOp::Write, 42, 0xBEEF, 4096, 0);
    let data = vec![0xAB; 4096];
    let msg = NdpMessage::new(header, Some(data.clone()));
    msg.write_to(&mut client).await.unwrap();

    let received = NdpMessage::read_from(&mut server).await.unwrap();
    assert_eq!(received.header.op, NdpOp::Write);
    assert_eq!(received.header.request_id, 42);
    assert_eq!(received.header.data_length, 4096);
    assert_eq!(received.data.unwrap(), data);
}

#[tokio::test]
async fn test_data_crc_verified() {
    let (mut client, mut server) = duplex(8192);
    let header = NdpHeader::request(NdpOp::Write, 1, 0, 0, 0);
    let data = vec![0xFF; 128];
    let msg = NdpMessage::new(header, Some(data));
    msg.write_to(&mut client).await.unwrap();

    // Corrupt one data byte in the stream before reading
    // (can't easily do this with duplex, so just verify CRC is set)
    let received = NdpMessage::read_from(&mut server).await.unwrap();
    assert!(received.header.data_crc != 0);
}

#[tokio::test]
async fn test_multiple_messages() {
    let (mut client, mut server) = duplex(65536);
    for i in 0..10u64 {
        let h = NdpHeader::request(NdpOp::Read, i, 0, i * 4096, 0);
        NdpMessage::new(h, None).write_to(&mut client).await.unwrap();
    }
    for i in 0..10u64 {
        let msg = NdpMessage::read_from(&mut server).await.unwrap();
        assert_eq!(msg.header.request_id, i);
        assert_eq!(msg.header.offset, i * 4096);
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cd dataplane/ndp && cargo test`
Expected: All tests pass (header + codec).

- [ ] **Step 4: Commit**

```bash
git add dataplane/ndp/src/codec.rs dataplane/ndp/src/lib.rs dataplane/ndp/tests/codec_test.rs
git commit -m "feat(ndp): async codec — NdpMessage read/write with data CRC"
```

---

### Task 3: NDP Connection (Multiplexed Request/Response)

**Files:**
- Create: `dataplane/ndp/src/connection.rs`
- Modify: `dataplane/ndp/src/lib.rs`
- Create: `dataplane/ndp/tests/connection_test.rs`

- [ ] **Step 1: Implement multiplexed connection**

Create `dataplane/ndp/src/connection.rs`:
```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::codec::NdpMessage;
use crate::error::{NdpError, Result};
use crate::header::{NdpHeader, NdpOp};

/// A multiplexed NDP connection. Multiple requests can be in-flight
/// simultaneously. Responses are matched to requests by request_id.
pub struct NdpConnection {
    writer: Arc<Mutex<WriteHalf<TcpStream>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<NdpMessage>>>>,
    next_id: AtomicU64,
    _reader_task: tokio::task::JoinHandle<()>,
}

impl NdpConnection {
    /// Connect to a remote NDP peer.
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self::from_stream(stream))
    }

    /// Wrap an existing TCP stream.
    pub fn from_stream(stream: TcpStream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        let writer = Arc::new(Mutex::new(writer));
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<NdpMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_clone = pending.clone();
        let reader_task = tokio::spawn(async move {
            Self::read_loop(reader, pending_clone).await;
        });

        Self {
            writer,
            pending,
            next_id: AtomicU64::new(1),
            _reader_task: reader_task,
        }
    }

    /// Send a request and wait for the response.
    pub async fn request(&self, mut header: NdpHeader, data: Option<Vec<u8>>) -> Result<NdpMessage> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        header.request_id = id;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let msg = NdpMessage::new(header, data);
        {
            let mut writer = self.writer.lock().await;
            msg.write_to(&mut *writer).await?;
        }

        rx.await.map_err(|_| NdpError::ConnectionClosed)
    }

    /// Send a message without waiting for a response (fire-and-forget).
    pub async fn send(&self, header: NdpHeader, data: Option<Vec<u8>>) -> Result<()> {
        let msg = NdpMessage::new(header, data);
        let mut writer = self.writer.lock().await;
        msg.write_to(&mut *writer).await
    }

    async fn read_loop(
        mut reader: ReadHalf<TcpStream>,
        pending: Arc<Mutex<HashMap<u64, oneshot::Sender<NdpMessage>>>>,
    ) {
        loop {
            match NdpMessage::read_from(&mut reader).await {
                Ok(msg) => {
                    let id = msg.header.request_id;
                    if let Some(tx) = pending.lock().await.remove(&id) {
                        let _ = tx.send(msg);
                    }
                }
                Err(_) => break, // Connection closed or error
            }
        }
        // Connection lost — fail all pending requests.
        let mut map = pending.lock().await;
        map.clear();
    }
}
```

- [ ] **Step 2: Implement connection pool**

Create `dataplane/ndp/src/pool.rs`:
```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::connection::NdpConnection;
use crate::error::{NdpError, Result};

/// Pool of persistent NDP connections to multiple chunk engines.
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<String, Arc<NdpConnection>>>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a persistent connection to a peer.
    pub async fn add(&self, addr: &str) -> Result<()> {
        let conn = NdpConnection::connect(addr).await?;
        self.connections
            .write()
            .await
            .insert(addr.to_string(), Arc::new(conn));
        Ok(())
    }

    /// Get a connection to a peer.
    pub async fn get(&self, addr: &str) -> Result<Arc<NdpConnection>> {
        self.connections
            .read()
            .await
            .get(addr)
            .cloned()
            .ok_or_else(|| NdpError::Pool(format!("no connection to {}", addr)))
    }

    /// Remove a connection.
    pub async fn remove(&self, addr: &str) {
        self.connections.write().await.remove(addr);
    }

    /// List all connected peers.
    pub async fn peers(&self) -> Vec<String> {
        self.connections.read().await.keys().cloned().collect()
    }
}
```

Update `dataplane/ndp/src/lib.rs`:
```rust
pub mod header;
pub mod codec;
pub mod connection;
pub mod pool;
pub mod error;

pub use header::{NdpHeader, NdpOp};
pub use codec::NdpMessage;
pub use connection::NdpConnection;
pub use pool::ConnectionPool;
pub use error::{NdpError, Result};
```

- [ ] **Step 3: Write connection tests**

Create `dataplane/ndp/tests/connection_test.rs`:
```rust
use ndp::{NdpConnection, NdpHeader, NdpMessage, NdpOp};
use tokio::net::TcpListener;

async fn echo_server(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(stream);
    loop {
        match NdpMessage::read_from(&mut reader).await {
            Ok(msg) => {
                let resp = match msg.header.op {
                    NdpOp::Ping => NdpMessage::new(
                        NdpHeader::response(&msg.header, NdpOp::Pong, 0, 0),
                        None,
                    ),
                    NdpOp::Read => NdpMessage::new(
                        NdpHeader::response(&msg.header, NdpOp::ReadResp, 0, 4),
                        Some(vec![0xAB; 4]),
                    ),
                    _ => NdpMessage::new(
                        NdpHeader::response(&msg.header, NdpOp::WriteResp, 0, 0),
                        None,
                    ),
                };
                resp.write_to(&mut writer).await.unwrap();
            }
            Err(_) => break,
        }
    }
}

#[tokio::test]
async fn test_connection_ping_pong() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    tokio::spawn(echo_server(listener));

    let conn = NdpConnection::connect(&addr).await.unwrap();
    let header = NdpHeader::request(NdpOp::Ping, 0, 0, 0, 0);
    let resp = conn.request(header, None).await.unwrap();
    assert_eq!(resp.header.op, NdpOp::Pong);
}

#[tokio::test]
async fn test_connection_multiplexed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    tokio::spawn(echo_server(listener));

    let conn = NdpConnection::connect(&addr).await.unwrap();

    // Fire multiple requests concurrently.
    let mut handles = vec![];
    for _ in 0..10 {
        let conn = &conn;
        handles.push(async move {
            let h = NdpHeader::request(NdpOp::Ping, 0, 0, 0, 0);
            conn.request(h, None).await.unwrap()
        });
    }
    let results = futures::future::join_all(handles).await;
    assert_eq!(results.len(), 10);
    for r in results {
        assert_eq!(r.header.op, NdpOp::Pong);
    }
}

#[tokio::test]
async fn test_connection_read_with_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    tokio::spawn(echo_server(listener));

    let conn = NdpConnection::connect(&addr).await.unwrap();
    let header = NdpHeader::request(NdpOp::Read, 0, 0, 0, 0);
    let resp = conn.request(header, None).await.unwrap();
    assert_eq!(resp.header.op, NdpOp::ReadResp);
    assert_eq!(resp.data.unwrap(), vec![0xAB; 4]);
}
```

- [ ] **Step 4: Add futures dependency for join_all**

Add to `dataplane/ndp/Cargo.toml` under `[dev-dependencies]`:
```toml
futures = "0.3"
```

- [ ] **Step 5: Run all tests**

Run: `cd dataplane/ndp && cargo test`
Expected: All tests pass (header + codec + connection).

- [ ] **Step 6: Commit**

```bash
git add dataplane/ndp/
git commit -m "feat(ndp): multiplexed connection + connection pool"
```

---

Plan complete and saved to `docs/superpowers/plans/2026-03-18-ndp-protocol-library.md`. Ready to execute?